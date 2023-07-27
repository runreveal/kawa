package multi

import (
	"context"

	"github.com/runreveal/chta"
)

type MultiDestination[T any] struct {
	wrapped []chta.Destination[T]
}

// TODO: options for ack behavior?
func NewMultiDestination[T any](dests []chta.Destination[T]) MultiDestination[T] {
	return MultiDestination[T]{
		wrapped: dests,
	}
}

func (md MultiDestination[T]) Send(ctx context.Context, ack func(), msgs ...chta.Message[T]) error {
	if ack != nil {
		ack = ackFn(ack, len(md.wrapped))
	}
	for _, d := range md.wrapped {
		err := d.Send(ctx, ack, msgs...)
		if err != nil {
			return err
		}
	}
	return nil
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
