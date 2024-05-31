package batch

import (
	"context"
	"errors"
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
	flusher     Flusher[T]
	flushq      chan func()
	flushlen    int
	flushfreq   time.Duration
	flusherr    chan error
	stopTimeout time.Duration

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
	StopTimeout      time.Duration
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

func StopTimeout(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.StopTimeout = d
	}
}

// NewDestination instantiates a new batcher.
func NewDestination[T any](f Flusher[T], opts ...OptFunc) *Destination[T] {
	cfg := Opts{
		FlushLength:      100,
		FlushFrequency:   1 * time.Second,
		FlushParallelism: 2,
		StopTimeout:      5 * time.Second,
	}

	for _, o := range opts {
		o(&cfg)
	}

	// TODO: validate here
	if cfg.FlushParallelism < 1 {
		panic("FlushParallelism must be greater than or equal to 1")
	}
	if cfg.StopTimeout < 0 {
		cfg.StopTimeout = 0
	}

	return &Destination[T]{
		flushlen:    cfg.FlushLength,
		flushq:      make(chan func(), cfg.FlushParallelism),
		flusherr:    make(chan error, cfg.FlushParallelism),
		flusher:     f,
		flushfreq:   cfg.FlushFrequency,
		stopTimeout: cfg.StopTimeout,

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
				d.flush(ctx)
				setTimer = true
			}
		case tEpoch := <-epochC:
			// if we haven't flushed yet this epoch, then flush, otherwise ignore
			if tEpoch == epoch {
				epoch++
				d.flush(ctx)
				setTimer = true
			}
		case err = <-d.flusherr:
			slog.Error("flush error", "error", err)
			break loop

		case <-ctx.Done():
			if len(d.buf) > 0 {
				// optimistic final flush.
				// launched in a goroutine to avoid deadlock acuqiring a flushq slot
				// might be better to return and not try to write this batch.
				go d.flush(context.Background())
			}
			break loop
		}
	}

	if len(d.flushq) == 0 {
		return err
	}

	slog.Info("stopping batcher. waiting for remaining flushes to finish.", "len", len(d.flushq))
timeout:
	for {
		// Wait for flushes to finish
		select {
		case <-time.After(d.stopTimeout):
			break timeout
		case e2 := <-d.flusherr:
			if e2 != nil {
				slog.Info("flush error", "error", err)
				if err == nil {
					err = e2
				}
			}
		}
	}
	if len(d.flushq) == 0 {
		return err
	}

drain:
	// flushes still active after timeout
	// cancel them.
	for {
		select {
		case cncl := <-d.flushq:
			cncl()
		default:
			break drain
		}
	}
	err = errDeadlock
	return err
}

var errDeadlock = errors.New("batcher: flushes timed out waiting for completion after context stopped.")

func (d *Destination[T]) flush(ctx context.Context) {
	// We make a new context here so that we can cancel the flush if the parent
	// context is canceled. It's important to use context.Background() here because
	// we don't want to propagate the parent context's cancelation to the flusher.
	// If we did, then the flusher would likely be canceled before it could
	// finish flushing.
	flctx, cancel := context.WithCancel(context.Background())
	// block until a slot is available, or until a timeout is reached in the
	// parent context
	select {
	case d.flushq <- cancel:
	case <-ctx.Done():
		cancel()
		return
	}
	// Have to make a copy so these don't get overwritten
	msgs := make([]msgAck[T], len(d.buf))
	copy(msgs, d.buf)
	go func() {
		d.doflush(flctx, msgs)
		// clear flush slot
		cncl := <-d.flushq
		cncl()
	}()
	// Clear the buffer for the next batch
	d.buf = d.buf[:0]
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
