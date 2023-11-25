package runreveal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	batch "github.com/runreveal/kawa/x/batcher"
	"golang.org/x/exp/slog"
)

type Option func(*RunReveal)

func WithWebhookURL(url string) Option {
	return func(r *RunReveal) {
		r.webhookURL = url
	}
}

func WithHTTPClient(httpc *http.Client) Option {
	return func(r *RunReveal) {
		r.httpc = httpc
	}
}

type RunReveal struct {
	httpc   *http.Client
	batcher *batch.Destination[types.Event]

	webhookURL string
	reqConf    requests.Config
}

func New(opts ...Option) *RunReveal {
	ret := &RunReveal{
		httpc: http.DefaultClient,
	}
	for _, o := range opts {
		o(ret)
	}
	ret.batcher = batch.NewDestination[types.Event](ret,
		batch.FlushLength(25),
		batch.FlushFrequency(5*time.Second),
	)
	return ret
}

func (r *RunReveal) Run(ctx context.Context) error {
	if r.webhookURL == "" {
		return errors.New("missing webhook url")
	}

	r.reqConf = func(rb *requests.Builder) {
		rb.
			UserAgent("kawa").
			Accept("application/json").
			BaseURL(r.webhookURL).
			Header("Content-Type", "application/json")
	}

	return r.batcher.Run(ctx)
}

func (r *RunReveal) Send(ctx context.Context, ack func(), msgs ...kawa.Message[types.Event]) error {
	return r.batcher.Send(ctx, ack, msgs...)
}

func (r *RunReveal) newReq() *requests.Builder {
	return requests.New(r.reqConf)
}

// Flush sends the given messages of type kawa.Message[type.Event] to the RunReveal api
func (r *RunReveal) Flush(ctx context.Context, msgs []kawa.Message[types.Event]) error {
	batch := make([]json.RawMessage, len(msgs))
	var err error
	for i, msg := range msgs {
		batch[i], err = json.Marshal(msg.Value)
		if err != nil {
			slog.Error("error marshalling event", "err", err)
			continue
		}
	}

	// Send events to the webhookURL using POST
	err = r.newReq().BodyJSON(batch).Fetch(ctx)
	if err != nil {
		return err
	}
	// TODO: retries
	return nil
}
