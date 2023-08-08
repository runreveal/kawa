package gelf

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"time"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/types"
	batch "github.com/runreveal/kawa/x/batcher"
)

type Option func(*gelf)

func WithBatchSize(batchSize int) Option {
	return func(g *gelf) {
		g.batchSize = batchSize
	}
}

func WithHost(host string) Option {
	return func(g *gelf) {
		g.host = host
	}
}

func WithServiceAddr(serviceAddr string) Option {
	return func(g *gelf) {
		g.serviceAddr = serviceAddr
	}
}

func WithProtocol(protocol string) Option {
	return func(g *gelf) {
		g.protocol = protocol
	}
}

type gelf struct {
	batcher   *batch.Destination[types.Event]
	batchSize int

	host        string
	serviceAddr string
	protocol    string

	conn net.Conn
}

func New(opts ...Option) *gelf {
	host, _ := os.Hostname()
	ret := &gelf{}
	for _, o := range opts {
		o(ret)
	}
	if ret.protocol == "" || (ret.protocol != "udp" && ret.protocol != "tcp") {
		ret.protocol = "udp"
	}
	if ret.batchSize == 0 {
		ret.batchSize = 100
	}
	if ret.host == "" {
		ret.host = host
	}

	ret.batcher = batch.NewDestination[types.Event](ret,
		batch.FlushLength(ret.batchSize),
		batch.FlushFrequency(5*time.Second),
	)
	return ret
}

func (g *gelf) Run(ctx context.Context) error {
	if g.serviceAddr == "" {
		return errors.New("gelf service addr is not set")
	}
	return g.batcher.Run(ctx)
}

func (g *gelf) Send(ctx context.Context, ack func(), msgs ...kawa.Message[types.Event]) error {
	conn, err := net.Dial(g.protocol, g.serviceAddr)
	if err != nil {
		return err
	}
	g.conn = conn

	return g.batcher.Send(ctx, ack, msgs...)
}

func (g *gelf) Flush(ctx context.Context, msgs []kawa.Message[types.Event]) error {
	for _, msg := range msgs {
		gelfMessage, err := g.toGELF(msg.Value)
		if err != nil {
			return err
		}
		_, err = g.conn.Write(gelfMessage)
		if err != nil {
			return err
		}
		time.Sleep(1 * time.Millisecond)
	}

	return nil
}

func (g *gelf) toGELF(evt types.Event) ([]byte, error) {
	// GELF data format
	var (
		rawEvt    map[string]interface{}
		msgString string
	)
	err := json.Unmarshal(evt.RawLog, &rawEvt)
	if err != nil {
		msgString = string(evt.RawLog)
	} else if rawEvt["short_message"] != "" {
		msgString = rawEvt["short_message"].(string)
	} else if rawEvt["message"] != "" {
		msgString = rawEvt["message"].(string)
	} else {
		msgString = string(evt.RawLog)
	}

	seconds := float64(time.Now().Unix())
	nanoseconds := float64(time.Now().Nanosecond()) / 1e9

	gelfData := map[string]interface{}{
		"version":       "1.1",
		"host":          g.host,
		"timestamp":     seconds + nanoseconds,
		"short_message": msgString,
		"_content_type": evt.ContentType,
		"_source_type":  evt.SourceType,
		"_raw_log":      string(evt.RawLog),
		"_event_time":   evt.Timestamp.Format(time.RFC3339),
	}

	return json.Marshal(gelfData)
}
