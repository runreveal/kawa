package batch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/runreveal/kawa"
	"github.com/stretchr/testify/assert"
)

func TestAckChu(t *testing.T) {
	var called bool
	callMe := ackFn(func() { called = true }, 2)
	for i := 0; i < 2; i++ {
		callMe()
	}
	assert.True(t, called, "ack should be called")

	nilMe := ackFn(nil, 2)
	for i := 0; i < 2; i++ {
		// shouldn't panic
		nilMe()
	}
}

// func flushTest[T any](c context.Context, msgs []kawa.Message[T]) {
// 	for _, msg := range msgs {
// 		fmt.Println(msg.Value)
// 	}
// 	counter++
// }

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
	err := bat.Send(ctx, func() { close(done) }, writeMsgs...)
	assert.NoError(t, err)

	select {
	case err := <-errc:
		assert.NoError(t, err)
	case <-done:
	}
	cancel()

}

func TestBatchFlushTimeout(t *testing.T) {
	hMu := sync.Mutex{}
	handled := false

	var ff = func(c context.Context, msgs []kawa.Message[string]) error {
		for _, msg := range msgs {
			fmt.Println(msg.Value)
		}
		hMu.Lock()
		handled = true
		hMu.Unlock()
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

	time.Sleep(15 * time.Millisecond)

	hMu.Lock()
	assert.True(t, handled, "value should have been set!")
	hMu.Unlock()

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

		writeMsgs := []kawa.Message[string]{
			{Value: "hi"},
		}

		done := make(chan struct{})
		err := bat.Send(ctx, func() { close(done) }, writeMsgs...)
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
		err := bat.Send(ctx, func() { close(done) }, writeMsgs...)
		assert.NoError(t, err)
		cancel()

		err = <-errc
		assert.ErrorIs(t, err, errDeadlock, "should return deadlock error")
	})

	t.Run("handle errors when errors returned from flush", func(t *testing.T) {

		// This test deadlocks in failure
		// Should figure out how to write it better

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
			// will be blocked flushing
			{Value: "hi"},
			// will be stuck waiting for flush slot
			{Value: "hello"},
			// will be stuck waiting to write to msgs in Send
			{Value: "bonjour"},
		}

		done := make(chan struct{})
		err := bat.Send(ctx, func() { close(done) }, writeMsgs...)
		assert.NoError(t, err, "errors aren't returned from Send")

		cncl()

		// parallelism is 2, so max processing time is 220ms (110ms for the first
		// two msgs in parallel, and another 110ms for the third)
		// stop timeout of 90ms means we'll see the deadlock error
		err = <-errc
		assert.ErrorIs(t, err, errDeadlock)
	})

	t.Run("handle errors when errors returned from flush", func(t *testing.T) {

		// This test deadlocks in failure
		// Should figure out how to write it better

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
			// will be blocked flushing
			{Value: "hi"},
			// will be stuck waiting for flush slot
			{Value: "hello"},
			// will be stuck waiting to write to msgs in Send
			{Value: "bonjour"},
		}

		done := make(chan struct{})
		err := bat.Send(ctx, func() { close(done) }, writeMsgs...)
		assert.NoError(t, err, "errors aren't returned from Send")

		cncl()

		// parallelism is 2, so max processing time is 220ms (110ms for the first
		// two msgs in parallel, and another 110ms for the third)
		// stop timeout of 90ms means we'll see the deadlock error
		err = <-errc
		assert.ErrorIs(t, err, errDeadlock)
	})

	t.Run("dont deadlock on errors returned from flush with length 1", func(t *testing.T) {

		// This test deadlocks in failure
		// Should figure out how to write it better

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
			// will be blocked flushing
			{Value: "hi"},
			// will be stuck waiting for flush slot
			{Value: "hello"},
			// will be stuck waiting to write to msgs in Send
			{Value: "bonjour"},
		}

		done := make(chan struct{})
		err := bat.Send(ctx, func() { close(done) }, writeMsgs...)
		assert.NoError(t, err)

		err = <-errc
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
		err := bat.Send(ctx, func() { ackCount += 1 }, messages...)
		assert.NoError(t, err)
		time.Sleep(50 * time.Millisecond)
		cancel()

		err = <-errc
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
			// Just pass through the error
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

		// Wait for ack to happen
		select {
		case <-ackChan:
			// Success - message was acked
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
			// Just pass through the error
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
			// Return ErrDontAck, should not retry
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
			// Sleep longer than timeout to trigger deadline exceeded
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
			FlushTimeout(50*time.Millisecond), // Timeout shorter than flush operation
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
		// Should have tried 3 times, each time getting timeout
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
			MaxRetries(0), // No retries
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
