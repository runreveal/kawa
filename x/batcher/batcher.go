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

// ErrDontAck should be returned by ErrorHandlers when they wish to
// signal to the batcher to skip acking a message as delivered, but
// continue to process.  For example, if an error is retryable and
// will be retried upstream at the source if an ack is not received
// before some timeout.
var ErrDontAck = errors.New("Destination encountered a retryable error")

// Flusher is the core interface that the user of this package must implement
// to get the batching functionality.
// It takes a slice of messages and returns an error if the flush fails. It's
// expected to be run synchronously and only return once the flush is complete.
// The flusher MUST respond to the context being canceled and return an error
// if the context is canceled.  If no other error occured, then return the
// context error.
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
	flusher         Flusher[T]
	flushq          chan struct{}
	flushlen        int
	flushfreq       time.Duration
	flushcan        map[string]context.CancelFunc
	flushTimeout    time.Duration
	stopTimeout     time.Duration
	watchdogTimeout time.Duration

	errorHandler ErrorHandler[T]
	flusherr     chan error

	maxRetries        int
	initialBackoff    time.Duration
	maxBackoff        time.Duration
	backoffMultiplier float64
	isRetryable       func(error) bool

	messages chan msgAck[T]
	buf      []msgAck[T]

	count   int
	running bool
	syncMu  sync.Mutex
}

type OptFunc func(*Opts)

type Opts struct {
	FlushLength       int
	FlushFrequency    time.Duration
	FlushTimeout      time.Duration
	FlushParallelism  int
	StopTimeout       time.Duration
	WatchdogTimeout   time.Duration
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	IsRetryable       func(error) bool
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

func FlushTimeout(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.FlushTimeout = d
	}
}

func WatchdogTimeout(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.WatchdogTimeout = d
	}
}

func StopTimeout(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.StopTimeout = d
	}
}

func MaxRetries(n int) func(*Opts) {
	return func(opts *Opts) {
		opts.MaxRetries = n
	}
}

func InitialBackoff(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.InitialBackoff = d
	}
}

func MaxBackoff(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.MaxBackoff = d
	}
}

func BackoffMultiplier(m float64) func(*Opts) {
	return func(opts *Opts) {
		opts.BackoffMultiplier = m
	}
}

// IsRetryable takes a callback which will be called on the return value from
// your flush function. If it returns true, the flush will be retried,
// otherwise the error will be passed to your defined error handler.
func IsRetryable(fn func(error) bool) func(*Opts) {
	return func(opts *Opts) {
		opts.IsRetryable = fn
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
		FlushLength:       100,
		FlushFrequency:    1 * time.Second,
		FlushParallelism:  2,
		StopTimeout:       5 * time.Second,
		MaxRetries:        3,
		InitialBackoff:    500 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 2.0,
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
	if cfg.WatchdogTimeout < 0 {
		cfg.WatchdogTimeout = 0
	}
	if cfg.FlushTimeout < 0 {
		cfg.FlushTimeout = 0
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.InitialBackoff < 0 {
		cfg.InitialBackoff = 0
	}
	if cfg.MaxBackoff < 0 {
		cfg.MaxBackoff = 0
	}
	if cfg.BackoffMultiplier <= 0 {
		cfg.BackoffMultiplier = 1.0
	}

	d := &Destination[T]{
		flushlen:        cfg.FlushLength,
		flushq:          make(chan struct{}, cfg.FlushParallelism),
		flusher:         f,
		flushcan:        make(map[string]context.CancelFunc),
		flushfreq:       cfg.FlushFrequency,
		flushTimeout:    cfg.FlushTimeout,
		stopTimeout:     cfg.StopTimeout,
		watchdogTimeout: cfg.WatchdogTimeout,

		errorHandler: e,
		flusherr:     make(chan error, cfg.FlushParallelism),

		maxRetries:        cfg.MaxRetries,
		initialBackoff:    cfg.InitialBackoff,
		maxBackoff:        cfg.MaxBackoff,
		backoffMultiplier: cfg.BackoffMultiplier,
		isRetryable:       cfg.IsRetryable,

		messages: make(chan msgAck[T]),
	}

	return d
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

	var wdChan <-chan time.Time
	var wdTimer *time.Timer

	// We are using 3*parallelism for the buffer lenght
	// This is the theoretical maximum signals we would get if
	// #parallelism number of flushes started/completed/started again
	// before the channel would drain.  Memory cost is miniscule for
	// this buffer because it just holds struct{}
	wdResetC := make(chan struct{}, 3*len(d.flushq))
	if d.watchdogTimeout > 0 {
		wdTimer = time.NewTimer(d.watchdogTimeout)
		wdChan = wdTimer.C
	}

	var err error
loop:
	for {
		select {
		case <-wdChan:
			// If no flushes are in-flight, this is just an idle timeout, not a deadlock
			if len(d.flushq) == 0 {
				// Reset the watchdog and continue
				if wdTimer != nil {
					wdTimer.Reset(d.watchdogTimeout)
				}
				continue
			}
			// There are flushes in-flight that haven't completed - this is a real deadlock
			return errDeadlock

		case <-wdResetC:
			// A flush has started or completed, reset the watchdog
			if wdTimer != nil {
				if !wdTimer.Stop() {
					<-wdTimer.C
				}
				wdTimer.Reset(d.watchdogTimeout)
			}

		case msg := <-d.messages: // Here
			d.count++
			if setTimer {
				// copy the epoch to send on the chan after the timer fires
				epc := epoch
				time.AfterFunc(d.flushfreq, func() {
					epochC <- epc // Here
				})

				if wdTimer != nil {
					if !wdTimer.Stop() {
						<-wdTimer.C
					}
					wdTimer.Reset(d.watchdogTimeout)
				}

				setTimer = false
			}
			d.buf = append(d.buf, msg)
			if len(d.buf) >= d.flushlen {
				epoch++
				d.flush(ctx, wdResetC)
				setTimer = true
			}
		case tEpoch := <-epochC:
			// if we haven't flushed yet this epoch, then flush, otherwise ignore
			if tEpoch == epoch {
				epoch++
				d.flush(ctx, wdResetC)
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

var errDeadlock = errors.New("batcher: flushes timed out")

func (d *Destination[T]) flush(ctx context.Context, wdResetC chan<- struct{}) {
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
		// Flush slot acquired - signal watchdog reset
		select {
		case wdResetC <- struct{}{}:
		default:
			// Channel full, skip this reset signal
		}
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
		// Flush completed - signal watchdog reset
		select {
		case wdResetC <- struct{}{}:
		default:
			// Channel full, skip this reset signal
		}
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
	backoff := d.initialBackoff
	var flushErr error

	// Retry loop
	for attempt := 0; attempt <= d.maxRetries; attempt++ {
		// Create a fresh context with timeout for this attempt
		attemptCtx := ctx
		if d.flushTimeout > 0 {
			var cancel context.CancelFunc
			attemptCtx, cancel = context.WithTimeout(ctx, d.flushTimeout)
			defer cancel()
		}

		flushErr = d.flusher.Flush(attemptCtx, msgs)

		// Success case
		if flushErr == nil {
			if attempt > 0 {
				slog.Info("flush succeeded after retry", "attempt", attempt+1, "count", len(msgs))
			}
			break
		}

		// Failure case - check if we should retry
		slog.Debug("flush err", "error", flushErr, "attempt", attempt+1)

		// Check if error is retriable using the callback (if provided) - retry if we have attempts left
		if d.isRetryable != nil && d.isRetryable(flushErr) && attempt < d.maxRetries {
			slog.Warn(
				"flush failed, will retry",
				"error", flushErr,
				"attempt", attempt+1,
				"maxRetries", d.maxRetries+1,
				"backoff", backoff,
			)

			// Sleep with backoff before retrying
			select {
			case <-time.After(backoff):
				backoff = time.Duration(float64(backoff) * d.backoffMultiplier)
				if backoff > d.maxBackoff {
					backoff = d.maxBackoff
				}
				continue
			case <-ctx.Done():
				flushErr = ctx.Err()
				break
			}
		}

		// Non-retriable error or exhausted retries - exit retry loop
		slog.Error("flush failed after all retries", "error", flushErr, "attempts", attempt+1)
		break
	}

	var handlerErr error
	if flushErr != nil {
		handlerErr = d.errorHandler.HandleError(ctx, flushErr, msgs)
	} else {
		handlerErr = nil
	}

	// If error handler returns ErrDontAck, skip acking but continue running
	if errors.Is(handlerErr, ErrDontAck) {
		return
	}

	// Error handler resolved the error - ack the messages
	if handlerErr == nil {
		for _, ack := range acks {
			if ack != nil {
				ack()
			}
		}
		return
	}

	// Error handler returned an error - propagate it to stop the batcher
	d.flusherr <- handlerErr
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
