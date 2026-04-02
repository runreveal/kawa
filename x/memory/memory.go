package memory

import (
	"context"

	"github.com/runreveal/kawa"
)

// MemorySource is a [kawa.Source] that wraps a channel.
type MemorySource[T any] struct {
	MsgC <-chan T
}

// NewMemSource returns a new [MemorySource].
func NewMemSource[T any](in <-chan T) MemorySource[T] {
	return MemorySource[T]{
		MsgC: in,
	}
}

// Recv implements [kawa.Source] by receiving a message from the channel.
// If ctx.Done() is closed before a value is received,
// then Recv returns ctx.Err().
func (ms MemorySource[T]) Recv(ctx context.Context) (kawa.Message[T], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[T]{}, nil, ctx.Err()
	case v := <-ms.MsgC:
		return kawa.Message[T]{Value: v}, func() {}, nil
	}
}

// MemoryDestination is a [kawa.Destination] that wraps a channel.
type MemoryDestination[T any] struct {
	MsgC chan<- T
}

// NewMemDestination returns a new [MemoryDestination].
func NewMemDestination[T any](out chan<- T) MemoryDestination[T] {
	return MemoryDestination[T]{
		MsgC: out,
	}
}

// Send implements [kawa.Destination] by sending the message on the channel.
// If ctx.Done() is closed before the message is sent,
// then Send returns ctx.Err().
// If ack is not nil, then it will be called after the message is sent
// but before Send returns.
func (ms MemoryDestination[T]) Send(ctx context.Context, ack func(), msg kawa.Message[T]) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ms.MsgC <- msg.Value:
	}
	kawa.Ack(ack)
	return nil
}
