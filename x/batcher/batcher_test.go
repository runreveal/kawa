package batch

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/runreveal/kawa"
	"github.com/stretchr/testify/assert"
)

func TestBatcher(t *testing.T) {

	var ff = func(c context.Context, msgs []kawa.Message[string]) error {
		for _, msg := range msgs {
			fmt.Println(msg.Value)
		}
		return nil
	}

	bat := NewDestination[string](FlushFunc[string](ff), Raise[string](), FlushLength(1))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

	errc := make(chan error)

	go func(c context.Context, ec chan error) {
		ec <- bat.Run(c)
	}(ctx, errc)

	writeMsgs := []kawa.Message[string]{
		{Value: "hi"},
		{Value: "hello"},
		{Value: "bonjour"},
	}

	done := make(chan struct{})
	for i, m := range writeMsgs {
		var ack func()
		if i == len(writeMsgs)-1 {
			ack = func() { close(done) }
		}
		err := bat.Send(ctx, ack, m)
		assert.NoError(t, err)
	}

	select {
	case err := <-errc:
		assert.NoError(t, err)
	case <-done:
	}
	cancel()

}

func TestBatchFlushTimeout(t *testing.T) {
	handled := make(chan struct{})

	var ff = func(c context.Context, msgs []kawa.Message[string]) error {
		for _, msg := range msgs {
			fmt.Println(msg.Value)
		}
		close(handled)
		return nil
	}

	bat := NewDestination[string](
		FlushFunc[string](ff),
		Raise[string](),
		FlushFrequency(1*time.Millisecond),
		FlushLength(2),
		StopTimeout(10*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

	errc := make(chan error)

	go func(c context.Context, ec chan error) {
		ec <- bat.Run(c)
	}(ctx, errc)

	done := make(chan struct{})
	err := bat.Send(ctx, func() { close(done) }, kawa.Message[string]{Value: "hi"})
	assert.NoError(t, err)

	select {
	case <-handled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("flush not called in time")
	}

	select {
	case err := <-errc:
		assert.NoError(t, err)
	case <-done:
	}
	cancel()

}

func TestBatcherErrors(t *testing.T) {
	flushErr := errors.New("flush error")
	var ff = func(c context.Context, msgs []kawa.Message[string]) error {
		return flushErr
	}

	t.Run("flush errors return from run", func(t *testing.T) {
		bat := NewDestination[string](FlushFunc[string](ff), Raise[string](), FlushLength(1))
		errc := make(chan error)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		done := make(chan struct{})
		err := bat.Send(ctx, func() { close(done) }, kawa.Message[string]{Value: "hi"})
		assert.NoError(t, err)

		select {
		case err := <-errc:
			assert.EqualError(t, err, "flush error")
		case <-done:
		}
		cancel()
	})

	t.Run("cancellation works", func(t *testing.T) {
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			return nil
		}
		bat := NewDestination[string](FlushFunc[string](ff), Raise[string](), FlushLength(1))
		errc := make(chan error)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		cancel()
		err := <-errc
		assert.ErrorIs(t, err, nil, "should return nil since no errors in flush")
	})

	t.Run("deadlock cancellation", func(t *testing.T) {

		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			<-c.Done()
			return nil
		}
		bat := NewDestination[string](FlushFunc[string](ff), Raise[string](), FlushLength(1), StopTimeout(10*time.Millisecond))

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		errc := make(chan error)

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		writeMsgs := []kawa.Message[string]{
			// will be blocked flushing
			{Value: "hi"},
			// will be stuck waiting for flush slot
			{Value: "hello"},
			// will be stuck waiting to write to msgs in Send
			{Value: "bonjour"},
		}

		done := make(chan struct{})
		for i, m := range writeMsgs {
			var ack func()
			if i == len(writeMsgs)-1 {
				ack = func() { close(done) }
			}
			err := bat.Send(ctx, ack, m)
			assert.NoError(t, err)
		}
		cancel()

		err := <-errc
		assert.ErrorIs(t, err, errDeadlock, "should return deadlock error")
	})

	t.Run("handle errors when errors returned from flush", func(t *testing.T) {

		flushErr := errors.New("flush error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			time.Sleep(110 * time.Millisecond)
			return flushErr
		}
		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			assert.ErrorIs(t, err, flushErr)
			return err
		})
		bat := NewDestination[string](
			FlushFunc[string](ff),
			errHandler,
			FlushLength(2),
			FlushParallelism(2),
			StopTimeout(90*time.Millisecond),
		)
		errc := make(chan error)

		ctx, cncl := context.WithCancel(context.Background())

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		writeMsgs := []kawa.Message[string]{
			{Value: "hi"},
			{Value: "hello"},
			{Value: "bonjour"},
		}

		done := make(chan struct{})
		for i, m := range writeMsgs {
			var ack func()
			if i == len(writeMsgs)-1 {
				ack = func() { close(done) }
			}
			err := bat.Send(ctx, ack, m)
			assert.NoError(t, err, "errors aren't returned from Send")
		}

		cncl()

		err := <-errc
		assert.ErrorIs(t, err, errDeadlock)
	})

	t.Run("dont deadlock on errors returned from flush with length 1", func(t *testing.T) {

		flushErr := errors.New("flush error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			time.Sleep(5 * time.Millisecond)
			return flushErr
		}
		bat := NewDestination[string](FlushFunc[string](ff), Raise[string](), FlushLength(1), FlushParallelism(2),
			StopTimeout(100*time.Millisecond))
		errc := make(chan error)

		ctx := context.Background()

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		writeMsgs := []kawa.Message[string]{
			{Value: "hi"},
			{Value: "hello"},
			{Value: "bonjour"},
		}

		done := make(chan struct{})
		for i, m := range writeMsgs {
			var ack func()
			if i == len(writeMsgs)-1 {
				ack = func() { close(done) }
			}
			err := bat.Send(ctx, ack, m)
			assert.NoError(t, err)
		}

		err := <-errc
		assert.ErrorIs(t, err, flushErr)
	})

	t.Run("Don't ack messages if flush handler returns ErrDontAck", func(t *testing.T) {
		var retryHandler = func(ctx context.Context, err error, msgs []kawa.Message[string]) error {
			return ErrDontAck
		}
		bat := NewDestination[string](
			FlushFunc[string](ff),
			ErrorFunc[string](retryHandler),
			FlushLength(1),
			FlushParallelism(1),
		)
		errc := make(chan error)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		messages := []kawa.Message[string]{
			{Value: "one"},
			{Value: "two"},
			{Value: "three"},
			{Value: "ten"},
		}

		ackCount := 0
		for i, m := range messages {
			var ack func()
			if i == len(messages)-1 {
				ack = func() { ackCount++ }
			}
			err := bat.Send(ctx, ack, m)
			assert.NoError(t, err)
		}
		time.Sleep(50 * time.Millisecond)
		cancel()

		err := <-errc
		assert.ErrorIs(t, err, nil)

		assert.Equal(t, 0, ackCount)
	})
}

func TestBatcherRetry(t *testing.T) {
	t.Run("retry on retriable error and succeed", func(t *testing.T) {
		var attemptCount atomic.Int32
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			count := attemptCount.Add(1)
			if count < 3 {
				return errors.New("temporary error")
			}
			return nil
		}

		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			return err
		})

		bat := NewDestination[string](
			FlushFunc[string](ff),
			errHandler,
			FlushLength(1),
			MaxRetries(3),
			InitialBackoff(10*time.Millisecond),
			IsRetryable(func(err error) bool {
				return err != nil && err.Error() == "temporary error"
			}),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		ackChan := make(chan struct{})
		err := bat.Send(ctx, func() { close(ackChan) }, kawa.Message[string]{Value: "hi"})
		assert.NoError(t, err)

		select {
		case <-ackChan:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("timeout waiting for ack")
		}

		cancel()
		err = <-errc
		assert.NoError(t, err)
		assert.Equal(t, int32(3), attemptCount.Load(), "should have made 3 attempts")
	})

	t.Run("retry exhausts max attempts", func(t *testing.T) {
		var attemptCount atomic.Int32
		flushErr := errors.New("persistent error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			attemptCount.Add(1)
			return flushErr
		}

		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			return err
		})

		bat := NewDestination[string](
			FlushFunc[string](ff),
			errHandler,
			FlushLength(1),
			MaxRetries(2),
			InitialBackoff(5*time.Millisecond),
			IsRetryable(func(err error) bool {
				return err != nil && err.Error() == "persistent error"
			}),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		ackCount := 0
		err := bat.Send(ctx, func() { ackCount++ }, kawa.Message[string]{Value: "hi"})
		assert.NoError(t, err)

		err = <-errc
		assert.ErrorIs(t, err, flushErr)
		assert.Equal(t, int32(3), attemptCount.Load(), "should have made 3 attempts (initial + 2 retries)")
		assert.Equal(t, 0, ackCount, "should not have acked")
	})

	t.Run("no retry on non-retriable error", func(t *testing.T) {
		var attemptCount atomic.Int32
		flushErr := errors.New("non-retriable error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			attemptCount.Add(1)
			return flushErr
		}

		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			return err
		})

		bat := NewDestination[string](
			FlushFunc[string](ff),
			errHandler,
			FlushLength(1),
			MaxRetries(3),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		err := bat.Send(ctx, nil, kawa.Message[string]{Value: "hi"})
		assert.NoError(t, err)

		err = <-errc
		assert.ErrorIs(t, err, flushErr)
		assert.Equal(t, int32(1), attemptCount.Load(), "should have made only 1 attempt")
	})

	t.Run("retry with exponential backoff", func(t *testing.T) {
		attemptTimes := []time.Time{}
		flushErr := errors.New("error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			attemptTimes = append(attemptTimes, time.Now())
			return flushErr
		}

		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			return err
		})

		bat := NewDestination[string](
			FlushFunc[string](ff),
			errHandler,
			FlushLength(1),
			MaxRetries(2),
			InitialBackoff(50*time.Millisecond),
			BackoffMultiplier(2.0),
			IsRetryable(func(err error) bool {
				return err != nil
			}),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		err := bat.Send(ctx, nil, kawa.Message[string]{Value: "hi"})
		assert.NoError(t, err)

		err = <-errc
		assert.ErrorIs(t, err, flushErr)
		assert.Equal(t, 3, len(attemptTimes), "should have 3 attempts")

		// Check backoff timing (with some tolerance)
		timeBetween1and2 := attemptTimes[1].Sub(attemptTimes[0])
		assert.GreaterOrEqual(t, timeBetween1and2, 50*time.Millisecond)
		assert.Less(t, timeBetween1and2, 70*time.Millisecond)

		timeBetween2and3 := attemptTimes[2].Sub(attemptTimes[1])
		assert.GreaterOrEqual(t, timeBetween2and3, 100*time.Millisecond)
		assert.Less(t, timeBetween2and3, 120*time.Millisecond)
	})

	t.Run("ErrDontAck takes precedence over retry", func(t *testing.T) {
		var attemptCount atomic.Int32
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			attemptCount.Add(1)
			return errors.New("error")
		}

		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			return ErrDontAck
		})

		bat := NewDestination[string](
			FlushFunc[string](ff),
			errHandler,
			FlushLength(1),
			MaxRetries(3),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		ackCount := 0
		err := bat.Send(ctx, func() { ackCount++ }, kawa.Message[string]{Value: "hi"})
		assert.NoError(t, err)

		time.Sleep(50 * time.Millisecond)
		cancel()

		err = <-errc
		assert.NoError(t, err)
		assert.Equal(t, int32(1), attemptCount.Load(), "should have made only 1 attempt")
		assert.Equal(t, 0, ackCount, "should not have acked")
	})

	t.Run("retry respects FlushTimeout per attempt", func(t *testing.T) {
		var attemptCount atomic.Int32
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			attemptCount.Add(1)
			time.Sleep(100 * time.Millisecond)
			return c.Err()
		}

		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			return err
		})

		bat := NewDestination[string](
			FlushFunc[string](ff),
			errHandler,
			FlushLength(1),
			FlushTimeout(50*time.Millisecond),
			MaxRetries(2),
			InitialBackoff(10*time.Millisecond),
			IsRetryable(func(err error) bool {
				return errors.Is(err, context.DeadlineExceeded)
			}),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		err := bat.Send(ctx, nil, kawa.Message[string]{Value: "hi"})
		assert.NoError(t, err)

		err = <-errc
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, int32(3), attemptCount.Load(), "should have made 3 attempts")
	})

	t.Run("zero retries means no retry", func(t *testing.T) {
		var attemptCount atomic.Int32
		flushErr := errors.New("error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			attemptCount.Add(1)
			return flushErr
		}

		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			return err
		})

		bat := NewDestination[string](
			FlushFunc[string](ff),
			errHandler,
			FlushLength(1),
			MaxRetries(0),
			IsRetryable(func(err error) bool {
				return err != nil
			}),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		err := bat.Send(ctx, nil, kawa.Message[string]{Value: "hi"})
		assert.NoError(t, err)

		err = <-errc
		assert.ErrorIs(t, err, flushErr)
		assert.Equal(t, int32(1), attemptCount.Load(), "should have made only initial attempt")
	})
}

func TestWatchdog(t *testing.T) {
	t.Run("idle system does not trigger watchdog", func(t *testing.T) {
		var flushCount atomic.Int32
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			flushCount.Add(1)
			return nil
		}

		bat := NewDestination[string](
			FlushFunc[string](ff),
			Raise[string](),
			FlushLength(10),
			FlushFrequency(50*time.Millisecond),
			WatchdogTimeout(100*time.Millisecond),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		err := bat.Send(ctx, nil, kawa.Message[string]{Value: "message1"})
		assert.NoError(t, err)

		time.Sleep(70 * time.Millisecond)

		time.Sleep(150 * time.Millisecond)

		cancel()
		err = <-errc
		assert.NoError(t, err, "idle system should not trigger watchdog")
		assert.Equal(t, int32(1), flushCount.Load(), "should have flushed once")
	})

	t.Run("stuck flush with no new messages triggers watchdog", func(t *testing.T) {
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			time.Sleep(1 * time.Second)
			return nil
		}

		bat := NewDestination[string](
			FlushFunc[string](ff),
			Raise[string](),
			FlushLength(1),
			FlushTimeout(50*time.Millisecond),
			WatchdogTimeout(150*time.Millisecond),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		err := bat.Send(ctx, nil, kawa.Message[string]{Value: "message1"})
		assert.NoError(t, err)

		err = <-errc
		assert.ErrorIs(t, err, errDeadlock, "stuck flush should trigger watchdog")
	})

	t.Run("stuck flush with continuing message arrival triggers watchdog", func(t *testing.T) {
		var flushCount atomic.Int32
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			count := flushCount.Add(1)
			if count == 1 {
				time.Sleep(1 * time.Second)
				return nil
			}
			return nil
		}

		bat := NewDestination[string](
			FlushFunc[string](ff),
			Raise[string](),
			FlushLength(1),
			FlushParallelism(2),
			FlushTimeout(50*time.Millisecond),
			WatchdogTimeout(200*time.Millisecond),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		err := bat.Send(ctx, nil, kawa.Message[string]{Value: "message1"})
		assert.NoError(t, err)

		time.Sleep(30 * time.Millisecond)

		for i := 0; i < 3; i++ {
			time.Sleep(80 * time.Millisecond)
			err := bat.Send(ctx, nil, kawa.Message[string]{Value: fmt.Sprintf("message%d", i+2)})
			assert.NoError(t, err)
		}

		err = <-errc
		assert.ErrorIs(t, err, errDeadlock, "stuck flush should trigger watchdog even with new messages")
	})

	t.Run("watchdog resets on flush completion", func(t *testing.T) {
		var flushCount atomic.Int32
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			flushCount.Add(1)
			time.Sleep(80 * time.Millisecond)
			return nil
		}

		bat := NewDestination[string](
			FlushFunc[string](ff),
			Raise[string](),
			FlushLength(1),
			WatchdogTimeout(200*time.Millisecond),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		errc := make(chan error)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		for i := 0; i < 3; i++ {
			err := bat.Send(ctx, nil, kawa.Message[string]{Value: fmt.Sprintf("message%d", i+1)})
			assert.NoError(t, err)
		}

		time.Sleep(300 * time.Millisecond)

		cancel()
		err := <-errc
		assert.NoError(t, err, "watchdog should not fire when flushes complete successfully")
		assert.Equal(t, int32(3), flushCount.Load(), "should have completed 3 flushes")
	})
}

func TestSendReturnsAfterRunExits(t *testing.T) {
	// When Run's context is canceled but the caller's Send context is still
	// alive, Send should return ErrNotRunning instead of blocking forever.

	slowFlush := func(ctx context.Context, msgs []kawa.Message[string]) error {
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	bat := NewDestination[string](
		FlushFunc[string](slowFlush),
		Raise[string](),
		FlushLength(100),
		FlushFrequency(10*time.Second),
	)

	runCtx, runCancel := context.WithCancel(context.Background())

	// Use a separate context for Send to simulate callers whose lifecycle
	// outlives the batcher (e.g. a long-lived processor goroutine).
	sendCtx, sendCancel := context.WithCancel(context.Background())
	defer sendCancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- bat.Run(runCtx)
	}()

	err := bat.Send(sendCtx, nil, kawa.Message[string]{Value: "msg1"})
	assert.NoError(t, err)

	runCancel()
	<-runErr

	// Now Send should return ErrNotRunning promptly, not block forever
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- bat.Send(sendCtx, nil, kawa.Message[string]{Value: "msg2"})
	}()

	select {
	case err := <-sendDone:
		assert.ErrorIs(t, err, ErrNotRunning)
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked after Run exited — done channel fix not working")
	}
}

func TestTimerGoroutineCleanup(t *testing.T) {
	// Verify that timer goroutines don't leak when Run exits before the
	// flush timer fires.

	bat := NewDestination[string](
		FlushFunc[string](func(_ context.Context, msgs []kawa.Message[string]) error {
			return nil
		}),
		Raise[string](),
		FlushLength(1000),
		FlushFrequency(50*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())

	runErr := make(chan error, 1)
	go func() {
		runErr <- bat.Run(ctx)
	}()

	err := bat.Send(ctx, nil, kawa.Message[string]{Value: "trigger"})
	assert.NoError(t, err)

	// Cancel before the flush timer fires
	cancel()
	<-runErr

	// Wait for any pending timer callbacks to fire and resolve
	time.Sleep(200 * time.Millisecond)

	// Verify Send returns immediately instead of blocking on a dead batcher.
	sendErr := bat.Send(context.Background(), nil, kawa.Message[string]{Value: "after"})
	assert.ErrorIs(t, sendErr, ErrNotRunning)
}
