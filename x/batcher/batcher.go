package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"log/slog"

	"github.com/runreveal/kawa"
	"github.com/segmentio/ksuid"
)

type Flusher[T any] interface {
	Flush(context.Context, []kawa.Message[T]) error
}

type FlushFunc[T any] func(context.Context, []kawa.Message[T]) error

func (ff FlushFunc[T]) Flush(c context.Context, msgs []kawa.Message[T]) error {
	return ff(c, msgs)
}

type ErrorHandler[T any] interface {
	HandleError(context.Context, error, []kawa.Message[T]) error
}

type ErrorFunc[T any] func(context.Context, error, []kawa.Message[T]) error

func (ef ErrorFunc[T]) HandleError(c context.Context, err error, msgs []kawa.Message[T]) error {
	return ef(c, err, msgs)
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
	flushq      chan struct{}
	flushlen    int
	flushfreq   time.Duration
	flushcan    map[string]context.CancelFunc
	stopTimeout time.Duration

	errorHandler ErrorHandler[T]
	flusherr     chan error

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

func DiscardHandler[T any]() ErrorHandler[T] {
	return ErrorFunc[T](func(context.Context, error, []kawa.Message[T]) error { return nil })
}

func Raise[T any]() ErrorHandler[T] {
	return ErrorFunc[T](func(_ context.Context, err error, _ []kawa.Message[T]) error { return err })
}

// NewDestination instantiates a new batcher.
func NewDestination[T any](f Flusher[T], e ErrorHandler[T], opts ...OptFunc) *Destination[T] {
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
	if e == nil {
		panic("ErrorHandler must not be nil")
	}
	if cfg.StopTimeout < 0 {
		cfg.StopTimeout = 0
	}

	return &Destination[T]{
		flushlen:    cfg.FlushLength,
		flushq:      make(chan struct{}, cfg.FlushParallelism),
		flusher:     f,
		flushcan:    make(map[string]context.CancelFunc),
		flushfreq:   cfg.FlushFrequency,
		stopTimeout: cfg.StopTimeout,

		errorHandler: e,
		flusherr:     make(chan error, cfg.FlushParallelism),

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
		case d.messages <- msgAck[T]{msg: m, ack: callMe}: // Here
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

	var err error
loop:
	for {
		select {
		case msg := <-d.messages: // Here
			d.count++
			if setTimer {
				// copy the epoch to send on the chan after the timer fires
				epc := epoch
				time.AfterFunc(d.flushfreq, func() {
					epochC <- epc // Here
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
		case <-ctx.Done():
			// on shutdown, don't attempt final flush even if buffer is not empty
			break loop
		case err = <-d.flusherr:
			break loop
		}
	}

	// we're done, no flushes in flight
	if len(d.flushq) == 0 {
		return err
	}

	slog.Info("stopping batcher. waiting for remaining flushes to finish.", "len", len(d.flushq))
	for i := 10 * time.Millisecond; i < d.stopTimeout; i = i + 10*time.Millisecond {
		if len(d.flushq) == 0 {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	// flushes still active after timeout
	// cancel them.
	d.syncMu.Lock()
	for k, v := range d.flushcan {
		v()
		fmt.Println("timeout cancel for id", k)
	}
	d.syncMu.Unlock()
	return errDeadlock
}

var errDeadlock = errors.New("batcher: flushes timed out waiting for completion after context stopped.")

func (d *Destination[T]) flush(ctx context.Context) {
	// We make a new context here so that we can cancel the flush if the parent
	// context is canceled. It's important to use context.Background() here because
	// we don't want to propagate the parent context's cancelation to the flusher.
	// If we did, then the flusher would likely be canceled before it could
	// finish flushing.
	flctx, cancel := context.WithCancel(context.Background())

	id := ksuid.New().String()
	d.syncMu.Lock()
	d.flushcan[id] = cancel
	d.syncMu.Unlock()

	// block until a slot is available, or until a timeout is reached in the
	// parent context
	select {
	case d.flushq <- struct{}{}:
	case <-ctx.Done():
		cancel()
		return
	}

	// Have to make a copy so these don't get overwritten
	msgs, acks := make([]kawa.Message[T], len(d.buf)), make([]func(), len(d.buf))
	for i, m := range d.buf {
		msgs[i] = m.msg
		acks[i] = m.ack
	}
	go func(id string, msgs []kawa.Message[T], acks []func()) {
		d.doflush(flctx, msgs, acks)
		// clear flush slot
		<-d.flushq
		// clear cancel
		d.syncMu.Lock()
		cncl := d.flushcan[id]
		delete(d.flushcan, id)
		d.syncMu.Unlock()
		cncl()
	}(id, msgs, acks)
	// Clear the buffer for the next batch
	d.buf = d.buf[:0]
}

func (d *Destination[T]) doflush(ctx context.Context, msgs []kawa.Message[T], acks []func()) {
	// This not ideal.
	// kawaMsgs := make([]kawa.Message[T], 0, len(msgs))
	// for _, m := range msgs {
	// 	kawaMsgs = append(kawaMsgs, m.msg)
	// }

	err := d.flusher.Flush(ctx, msgs)
	if err != nil {
		slog.Debug("flush err", "error", err)
		err := d.errorHandler.HandleError(ctx, err, msgs)
		if err != nil {
			d.flusherr <- err
			// if error handler returns an error, then we exit
			return
		}
	}

	for _, ack := range acks {
		if ack != nil {
			ack()
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
