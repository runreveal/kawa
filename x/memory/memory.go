package memory

import (
	"context"
	"errors"

	"github.com/runreveal/flow"
)

type MemorySource[T any] struct {
	MsgC <-chan T
}

func NewMemSource[T any](in <-chan T) MemorySource[T] {
	return MemorySource[T]{
		MsgC: in,
	}
}

func (ms MemorySource[T]) Recv(ctx context.Context) (flow.Message[T], func(), error) {
	select {
	case <-ctx.Done():
		return flow.Message[T]{}, nil, ctx.Err()
	case v := <-ms.MsgC:
		return flow.Message[T]{Value: v}, nil, nil
	}
}

type MemoryDestination[T any] struct {
	MsgC chan<- T
}

func NewMemDestination[T any](out chan<- T) MemoryDestination[T] {
	return MemoryDestination[T]{
		MsgC: out,
	}
}

func (ms MemoryDestination[T]) Send(ctx context.Context, ack func(), msgs ...flow.Message[T]) error {
	for _, msg := range msgs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ms.MsgC <- msg.Value:
			return nil
		}
	}
	return errors.New("unreachable")
}
