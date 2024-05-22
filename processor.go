package kawa

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("kawa/processor")
}

type Processor[T1, T2 any] struct {
	src         Source[T1]
	dst         Destination[T2]
	handler     Handler[T1, T2]
	parallelism int
	tracing     bool
	metrics     bool
}

type Config[T1, T2 any] struct {
	Source      Source[T1]
	Destination Destination[T2]
	Handler     Handler[T1, T2]
}

type Option func(*Options)

type Options struct {
	Parallelism int
	Tracing     bool
	Metrics     bool
}

func Parallelism(n int) func(*Options) {
	return func(o *Options) {
		o.Parallelism = n
	}
}

func Tracing(b bool) func(*Options) {
	return func(o *Options) {
		o.Tracing = b
	}
}

func Metrics(b bool) func(*Options) {
	return func(o *Options) {
		o.Metrics = b
	}
}

// New instantiates a new Processor.  `Processor.Run` must be called after calling `New`
// before events will be processed.
func New[T1, T2 any](c Config[T1, T2], opts ...Option) (*Processor[T1, T2], error) {
	if c.Source == nil || c.Destination == nil {
		return nil, errors.New("both Source and Destination required")
	}
	if c.Handler == nil {
		return nil, errors.New("handler required. Have you considered kawa.Pipe?")
	}
	var op Options
	for _, o := range opts {
		o(&op)
	}
	p := &Processor[T1, T2]{
		src:         c.Source,
		dst:         c.Destination,
		handler:     c.Handler,
		parallelism: op.Parallelism,
		tracing:     op.Tracing,
		metrics:     op.Metrics,
	}

	if p.parallelism < 1 {
		p.parallelism = 1
	}
	return p, nil
}

// handle runs the loop to receive, process and send messages.
func (p *Processor[T1, T2]) handle(ctx context.Context) error {
	for {
		ctx, handleSpan := tracer.Start(ctx, "kawa.processor.full")

		rctx, recvSpan := tracer.Start(ctx, "kawa.processor.src.recv")
		msg, ack, err := p.src.Recv(rctx)
		if err != nil {
			return fmt.Errorf("source: %w", err)
		}
		recvSpan.End()

		hctx, hdlSpan := tracer.Start(ctx, "kawa.processor.handler.handle")
		msgs, err := p.handler.Handle(hctx, msg)
		if err != nil {
			return fmt.Errorf("handler: %w", err)
		}
		hdlSpan.End()

		// If there are no messages, we don't need to send nil to destination
		if len(msgs) == 0 {
			handleSpan.End()
			Ack(ack)
			continue
		}

		sctx, sendSpan := tracer.Start(ctx, "kawa.processor.dst.send")
		err = p.dst.Send(sctx, ack, msgs...)
		if err != nil {
			return fmt.Errorf("destination: %w", err)
		}
		sendSpan.End()
		handleSpan.End()
	}
}

// Run is a blocking call, and runs until either the ctx is canceled, or an
// unrecoverable error is encountered. If any error is returned from a source,
// destination or the handler func, then it's wrapped and returned. If the
// passed-in context is canceled, this will not return the context.Canceled
// error to indicate a clean shutdown was successful.  Run will return
// ctx.Err() in other cases where context termination leads to shutdown of the
// processor.
func (p *Processor[T1, T2]) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(p.parallelism)
	ctx, cancel := context.WithCancel(ctx)
	errc := make(chan error, p.parallelism)

	for i := 0; i < p.parallelism; i++ {
		go func(c context.Context) {
			if e := p.handle(c); e != nil {
				errc <- e
			}
			wg.Done()
		}(ctx)
	}

	var err error
	select {
	case <-ctx.Done():
		// context was stopped by parent's cancel or parent timeout.
		// set err = ctx.Err() *only if* error is *not* context.Canceled,
		// because our contract defines that to be the way callers should stop
		// a worker cleanly.
		if !errors.Is(ctx.Err(), context.Canceled) {
			err = ctx.Err()
		}
	case err = <-errc:
		// All errors are fatal to this worker
		err = fmt.Errorf("worker: %w", err)
	}
	// Stop all the workers on shutdown.
	cancel()
	// TODO: capture errors thrown during shutdown?  if we do this, write local
	// err first. it represents first seen
	wg.Wait()
	close(errc)
	return err
}
