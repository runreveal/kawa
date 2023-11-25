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

func WithBatchSize(size int) Option {
	return func(r *RunReveal) {
		r.batchSize = size
	}
}

func WithFlushFrequency(t time.Duration) Option {
	return func(r *RunReveal) {
		r.flushFreq = t
	}
}

type RunReveal struct {
	httpc   *http.Client
	batcher *batch.Destination[types.Event]

	batchSize  int
	flushFreq  time.Duration
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

	if ret.batchSize <= 0 {
		ret.batchSize = 100
	}
	if ret.flushFreq <= 0 {
		ret.flushFreq = 15 * time.Second
	}

	ret.batcher = batch.NewDestination[types.Event](ret,
		batch.FlushLength(ret.batchSize),
		batch.FlushFrequency(ret.flushFreq),
		batch.FlushParallelism(2),
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

	slog.Info("sending batch to runreveal", "count", len(msgs))

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
		slog.Error("error sending batch to runreveal", "err", err)
		return err
	}
	// TODO: retries
	return nil
}
