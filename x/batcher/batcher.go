package batch

import (
	"context"
	"sync"
	"time"

	"github.com/runreveal/flow"
)

type Flusher[T any] interface {
	Flush(context.Context, []flow.Message[T]) error
}

type FlushFunc[T any] func(context.Context, []flow.Message[T]) error

func (ff FlushFunc[T]) Flush(c context.Context, msgs []flow.Message[T]) error {
	return ff(c, msgs)
}

type Destination[T any] struct {
	flusher   Flusher[T]
	flushq    chan struct{}
	flushlen  int
	flushfreq time.Duration
	flusherr  chan error
	flushwg   *sync.WaitGroup

	messages chan msgAck[T]
	buf      []msgAck[T]
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

// NewDestination instantiates a new batcher.  `Destination.Run` must be called
// after calling `New` before events will be processed in this destination. Not
// calling `Run` will likely end in a deadlock as the internal channel being
// written to by `Send` will not be getting read.
func NewDestination[T any](f Flusher[T], opts ...OptFunc) *Destination[T] {
	cfg := Opts{
		FlushLength:      100,
		FlushFrequency:   1 * time.Second,
		FlushParallelism: 2,
	}

	for _, o := range opts {
		o(&cfg)
	}

	return &Destination[T]{
		flushlen:  cfg.FlushLength,
		flushq:    make(chan struct{}, cfg.FlushParallelism),
		flusherr:  make(chan error, cfg.FlushParallelism),
		flusher:   f,
		flushwg:   &sync.WaitGroup{},
		flushfreq: cfg.FlushFrequency,

		messages: make(chan msgAck[T]),
	}

}

type msgAck[T any] struct {
	msg flow.Message[T]
	ack func()
}

// Send satisfies the flow.Destination interface and accepts messages to be
// buffered for flushing after the FlushLength limit is reached or the
// FlushFrequency timer fires, whichever comes first.
//
// Messages will not be acknowledged until they have been flushed successfully.
func (d *Destination[T]) Send(ctx context.Context, ack func(), msgs ...flow.Message[T]) error {
	if len(msgs) < 1 {
		return nil
	}

	callMe := ackFn(ack, len(msgs))

	for _, m := range msgs {
		select {
		case d.messages <- msgAck[T]{msg: m, ack: callMe}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (d *Destination[T]) Run(ctx context.Context) error {
	var err error
	var epoch uint64
	epochC := make(chan uint64)
	setTimer := true

loop:
	for {
		select {
		case msg := <-d.messages:
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
				err = d.flush(ctx)
				setTimer = true
				epoch++
				if err != nil {
					break loop
				}
			}
		case tEpoch := <-epochC:
			// if we haven't flushed yet this epoch, then flush, otherwise ignore
			if tEpoch == epoch {
				err = d.flush(ctx)
				setTimer = true
				epoch++
				if err != nil {
					break loop
				}
			}
		case err = <-d.flusherr:
			break loop
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		}
	}

	// Wait for in-flight flushes to finish
	// This must happen in the same goroutine as flushwg.Add
	d.flushwg.Wait()
	return err
}

func (d *Destination[T]) flush(ctx context.Context) error {
	select {
	// Acquire flush slot
	case d.flushq <- struct{}{}:
		// TODO: shallow copy?
		msgs := make([]msgAck[T], len(d.buf))
		copy(msgs, d.buf)
		// This must happen in the same goroutine as flushwg.Wait
		// do not push down into doflush
		d.flushwg.Add(1)
		go d.doflush(ctx, msgs)
		d.buf = d.buf[:0]
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (d *Destination[T]) doflush(ctx context.Context, msgs []msgAck[T]) {
	flowMsgs := make([]flow.Message[T], 0, len(msgs))
	for _, m := range msgs {
		flowMsgs = append(flowMsgs, m.msg)
	}

	err := d.flusher.Flush(ctx, flowMsgs)
	if err != nil {
		d.flusherr <- err
		d.flushwg.Done()
		return
	}

	for _, m := range msgs {
		if m.ack != nil {
			m.ack()
		}
	}

	// free waitgroup before clearing slot in queue to allow shutdown to proceed
	// before another flush starts if a shutdown is currently happening
	d.flushwg.Done()
	select {
	// clear flush slot
	case <-d.flushq:
	default:
		// this should be unreachable since we're the only reader
		panic("read of empty flushq")
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
