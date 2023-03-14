package flow

import (
	"context"
	"encoding/json"
)

type Attributes interface {
	Unwrap() Attributes
}

type Message[T any] struct {
	Key        string
	Value      T
	Topic      string
	Attributes Attributes
}

// NOTE: maybe have sources/destinations which are already message
// oriented have their own interface: Recv(ctx) []byte, func(), error
// combined with a generic deserializer source.
// This way you don't have to necessarily program in generics to write a
// source.
type Source[T any] interface {
	Recv(context.Context) (Message[T], func(), error)
}

type SourceFunc[T any] func(context.Context) (Message[T], func(), error)

func (sf SourceFunc[T]) Recv(ctx context.Context) (Message[T], func(), error) {
	return sf(ctx)
}

type Destination[T any] interface {
	Send(context.Context, func(), ...Message[T]) error
}

type DestinationFunc[T any] func(context.Context, func(), ...Message[T]) error

func (df DestinationFunc[T]) Send(ctx context.Context, ack func(), msgs ...Message[T]) error {
	return df(ctx, ack, msgs...)
}

type Handler[T1, T2 any] func(context.Context, Message[T1]) ([]Message[T2], error)

func Pipe[T any](ctx context.Context, msg Message[T]) ([]Message[T], error) {
	return []Message[T]{msg}, nil
}

type DeserFunc[T any] func([]byte) (T, error)

// BalancedSource handles rebalancing clients
// type BalancedSource[T any] interface {
// 	Listen(ctx context.Context) (Source[T], error)
// }

type ByteSource interface {
	Recv(context.Context) (Message[[]byte], func(), error)
}

func TransformUnmarshalJSON[T any](bs []byte) (T, error) {
	var val T
	err := json.Unmarshal(bs, &val)
	return val, err
}

type DeserializationSource[T any] struct {
	src   ByteSource
	xform func([]byte) (T, error)
}

func NewDeserSource[T any](src ByteSource, xform DeserFunc[T]) DeserializationSource[T] {
	return DeserializationSource[T]{
		src:   src,
		xform: xform,
	}
}

func (ds DeserializationSource[T]) Recv(ctx context.Context) (Message[T], func(), error) {
	msg, ack, err := ds.src.Recv(ctx)
	if err != nil {
		return Message[T]{}, ack, err
	}
	val, err := ds.xform(msg.Value)

	ret := Message[T]{
		Key:        msg.Key,
		Value:      val,
		Topic:      msg.Topic,
		Attributes: msg.Attributes,
	}
	return ret, ack, err
}
