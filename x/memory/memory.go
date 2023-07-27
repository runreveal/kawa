package memory

import (
	"context"
	"errors"

	"github.com/runreveal/kawa"
)

type MemorySource[T any] struct {
	MsgC <-chan T
}

func NewMemSource[T any](in <-chan T) MemorySource[T] {
	return MemorySource[T]{
		MsgC: in,
	}
}

func (ms MemorySource[T]) Recv(ctx context.Context) (kawa.Message[T], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[T]{}, nil, ctx.Err()
	case v := <-ms.MsgC:
		return kawa.Message[T]{Value: v}, nil, nil
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

func (ms MemoryDestination[T]) Send(ctx context.Context, ack func(), msgs ...kawa.Message[T]) error {
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
