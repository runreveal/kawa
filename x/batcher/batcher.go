package batch

import (
	"context"
	"sync"
	"time"

	"log/slog"

	"github.com/runreveal/kawa"
)

type Flusher[T any] interface {
	Flush(context.Context, []kawa.Message[T]) error
}

type FlushFunc[T any] func(context.Context, []kawa.Message[T]) error

func (ff FlushFunc[T]) Flush(c context.Context, msgs []kawa.Message[T]) error {
	return ff(c, msgs)
}

// Destination is a batching destination that will buffer messages until the
// FlushLength limit is reached or the FlushFrequency timer fires, whichever
// comes first.
//
// `Destination.Run` must be called after calling `New` before events will be
// processed in this destination. Not calling `Run` will likely end in a
// deadlock as the internal channel being written to by `Send` will not be
// getting read.
type Destination[T any] struct {
	flusher   Flusher[T]
	flushq    chan func()
	flushlen  int
	flushfreq time.Duration
	flusherr  chan error

	messages chan msgAck[T]
	buf      []msgAck[T]

	count   int
	running bool
	syncMu  sync.Mutex
}

type OptFunc func(*Opts)

type Opts struct {
	FlushLength      int
	FlushFrequency   time.Duration
	FlushParallelism int
}

func FlushFrequency(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.FlushFrequency = d
	}
}

func FlushLength(size int) func(*Opts) {
	return func(opts *Opts) {
		opts.FlushLength = size
	}
}

func FlushParallelism(n int) func(*Opts) {
	return func(opts *Opts) {
		opts.FlushParallelism = n
	}
}

// NewDestination instantiates a new batcher.
func NewDestination[T any](f Flusher[T], opts ...OptFunc) *Destination[T] {
	cfg := Opts{
		FlushLength:      100,
		FlushFrequency:   1 * time.Second,
		FlushParallelism: 2,
	}

	for _, o := range opts {
		o(&cfg)
	}

	// TODO: validate here

	return &Destination[T]{
		flushlen:  cfg.FlushLength,
		flushq:    make(chan func(), cfg.FlushParallelism),
		flusherr:  make(chan error, cfg.FlushParallelism),
		flusher:   f,
		flushfreq: cfg.FlushFrequency,

		messages: make(chan msgAck[T]),
	}

}

type msgAck[T any] struct {
	msg kawa.Message[T]
	ack func()
}

// Send satisfies the kawa.Destination interface and accepts messages to be
// buffered for flushing after the FlushLength limit is reached or the
// FlushFrequency timer fires, whichever comes first.
//
// Messages will not be acknowledged until they have been flushed successfully.
func (d *Destination[T]) Send(ctx context.Context, ack func(), msgs ...kawa.Message[T]) error {
	if len(msgs) < 1 {
		return nil
	}

	callMe := ackFn(ack, len(msgs))

	for _, m := range msgs {
		select {
		case d.messages <- msgAck[T]{msg: m, ack: callMe}:
		case <-ctx.Done():
			// TODO: one more flush?
			return ctx.Err()
		}
	}

	return nil
}

// Run starts the batching destination.  It must be called before messages will
// be processed and written to the underlying Flusher.
// Run will block until the context is canceled.
// Upon cancellation, Run will flush any remaining messages in the buffer and
// return any flush errors that occur
func (d *Destination[T]) Run(ctx context.Context) error {
	var err error
	var epoch uint64
	epochC := make(chan uint64)
	setTimer := true

	d.syncMu.Lock()
	if d.running {
		panic("already running")
	} else {
		d.running = true
	}
	d.syncMu.Unlock()

	ctx, cancel := context.WithCancel(ctx)

loop:
	for {
		select {
		case msg := <-d.messages:
			d.count++
			if setTimer {
				// copy the epoch to send on the chan after the timer fires
				epc := epoch
				time.AfterFunc(d.flushfreq, func() {
					epochC <- epc
				})
				setTimer = false
			}
			d.buf = append(d.buf, msg)
			if len(d.buf) >= d.flushlen {
				epoch++
				err = d.flush(ctx)
				if err != nil {
					slog.Error("flush error", "error", err)
					break loop
				}
				setTimer = true
			}
		case tEpoch := <-epochC:
			// if we haven't flushed yet this epoch, then flush, otherwise ignore
			if tEpoch == epoch {
				epoch++
				err = d.flush(ctx)
				if err != nil {
					break loop
				}
				setTimer = true
			}
		case err = <-d.flusherr:
			slog.Info("flush error", "error", err)
			break loop
		case <-ctx.Done():
			// TODO: how do we handle the final flush if
			// d.buf is not empty?
			err = ctx.Err()
			break loop
		}
	}

	cancel()

	return err
}

func (d *Destination[T]) flush(ctx context.Context) error {
	// We make a new context here so that we can cancel the flush if the parent
	// context is canceled. It's important to use context.Background() here because
	// we don't want to propagate the parent context's cancelation to the flusher.
	// If we did, then the flusher would likely be canceled before it could
	// finish flushing.
	subctx, cancel := context.WithCancel(context.Background())
	select {
	// Acquire flush slot
	case d.flushq <- cancel:
		// Have to make a copy so these don't get overwritten
		msgs := make([]msgAck[T], len(d.buf))
		copy(msgs, d.buf)
		go func() {
			d.doflush(subctx, msgs)
			// clear flush slot
			// this will block forever if we're shutting down
			cncl := <-d.flushq
			cncl()
		}()
		// Clear the buffer for the next batch
		d.buf = d.buf[:0]
	case <-ctx.Done():
	outer:
		// Stop active flushes
		for {
			select {
			case cncl := <-d.flushq:
				cncl()
			default:
				break outer
			}
		}
		return ctx.Err()
	}
	return nil
}

func (d *Destination[T]) doflush(ctx context.Context, msgs []msgAck[T]) {
	// This not ideal.
	kawaMsgs := make([]kawa.Message[T], 0, len(msgs))
	for _, m := range msgs {
		kawaMsgs = append(kawaMsgs, m.msg)
	}

	err := d.flusher.Flush(ctx, kawaMsgs)
	if err != nil {
		slog.Debug("flush err", "error", err)
	}
	if err != nil {
		d.flusherr <- err
		return
	}

	for _, m := range msgs {
		if m.ack != nil {
			m.ack()
		}
	}
}

// only call ack on last message acknowledgement
func ackFn(ack func(), num int) func() {
	ackChu := make(chan struct{}, num-1)
	for i := 0; i < num-1; i++ {
		ackChu <- struct{}{}
	}
	// bless you
	return func() {
		select {
		case <-ackChu:
		default:
			if ack != nil {
				ack()
			}
		}
	}
}
