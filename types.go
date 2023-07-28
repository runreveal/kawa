package kawa

import (
	"context"
	"encoding/json"
)

// Message is the data wrapper which accepts any serializable type as it's
// embedded Value as well as some other metadata.
type Message[T any] struct {
	// Key represents the key of this message.  This field is intended to be used
	// primarily as an input into sharding functions to determine how a message
	// should be routed within a topic.
	Key string
	// Value is the embedded value of this message.  It is the object of interest
	// to the users of this library.  It can be any serializable type so long as
	// the sources and destinations know how to serialize it.
	Value T
	// Topic indicates which topic this message came from (if applicable).  It
	// should not be used as a means to set the output topic for destinations.
	Topic string
	// Attributes are inspired by context.Context and are used as a means to pass
	// metadata from a source implementation through to a consumer.  See examples
	// for details.
	Attributes Attributes
}

type Attributes interface {
	Unwrap() Attributes
}

// Source defines the abstraction for which kawa consumes or receives messages
// from an external entity.  Most notable implementations are queues (Kafka,
// RabbitMQ, Redis), but anything which is message oriented could be made into
// a source (e.g. a newline-delimited-JSON file could conceivably be a source).
type Source[T any] interface {
	// Recv should block until Message is available to be returned from the
	// source.  Implementations _must_ listen on <-ctx.Done() and return
	// ctx.Err() if the context finishes while waiting for new messages.
	//
	// All errors which are retryable must be handled inside the Recv func, or
	// otherwise handled internally.  Any errors returned from Recv indicate a
	// fatal error to the processor, and the processor will terminate.  If you
	// want to be able to delegate the responsibility of deciding retryable
	// errors to the user of the Source, then allow the user to register a
	// callback, e.g. `IsRetryable(err error) bool`, on source instantiation.
	//
	// The second return value is the acknowlegement function.  Ack is called when
	// the message returned from Recv has been successfully written to it's
	// destination.  It should not be called twice.  Sources may panic in that
	// scenario as it indicates a logical flaw for delivery guarantees within the
	// program.
	//
	// In the case of sending to multiple destinations, or teeing the data stream
	// inside a processor's handler function, then the programmer must decide
	// themselves how to properly acknowledge the event, and recognize that
	// destinations will probably be acknowledging the message as well.
	Recv(context.Context) (Message[T], func(), error)
}

type SourceFunc[T any] func(context.Context) (Message[T], func(), error)

func (sf SourceFunc[T]) Recv(ctx context.Context) (Message[T], func(), error) {
	return sf(ctx)
}

// Destination defines the abstraction for writing messages to an external
// entity.  Most notable implementations are queues (Kafka, RabbitMQ, Redis),
// but anything which is message oriented could be made into a Destination
// (e.g. a newline-delimited-JSON file could conceivably be a Destination).
type Destination[T any] interface {
	// Send sends the passed in messages to the Destination. Implementations
	// _must_ listen on <-ctx.Done() and return ctx.Err() if the context finishes
	// while waiting to send messages.
	//
	// *Send need not be blocking*.  In the case of a non-blocking call to send,
	// it's expected that ack will be called _only after_ the message has been
	// successfully written to the Destination.
	//
	// All errors which are retryable must be handled inside the Send func, or
	// otherwise handled internally.  Any errors returned from Send indicate a
	// fatal error to the processor, and the processor will terminate.  If you
	// want to be able to delegate the responsibility of deciding retryable
	// errors to the user of the Destination, then allow the user to register a
	// callback, e.g. `IsRetryable(err error) bool`, when instantiating a
	// Destination.
	//
	// The second argument value is the acknowlegement function.  Ack is called
	// when the message has been successfully written to the Destination.  It
	// should not be called twice.  Sources may panic if ack is called twice as
	// it indicates a logical flaw for delivery guarantees within the program.
	//
	// In the case of sending to multiple destinations, or teeing the data stream
	// inside a processor's handler function, then the programmer must decide
	// themselves how to properly acknowledge the event, and recognize that
	// destinations will probably be acknowledging the message as well.
	Send(context.Context, func(), ...Message[T]) error
}

type DestinationFunc[T any] func(context.Context, func(), ...Message[T]) error

func (df DestinationFunc[T]) Send(ctx context.Context, ack func(), msgs ...Message[T]) error {
	return df(ctx, ack, msgs...)
}

// Handler defines a function which operates on a single event of type T1 and
// returns a list of events of type T2.  T1 and T2 may be equivalent types.
// Returning an empty slice and a nil error indicates that the message passed
// in was processed successfully, no output was necessary, and therefore should
// be acknowledged by the processor as having been processed successfully.
type Handler[T1, T2 any] interface {
	Handle(context.Context, Message[T1]) ([]Message[T2], error)
}

type HandlerFunc[T1, T2 any] func(context.Context, Message[T1]) ([]Message[T2], error)

func (hf HandlerFunc[T1, T2]) Handle(ctx context.Context, msg Message[T1]) ([]Message[T2], error) {
	return hf(ctx, msg)
}

func Pipe[T any]() Handler[T, T] {
	return pipe[T]{}
}

type pipe[T any] struct{}

func (p pipe[T]) Handle(ctx context.Context, msg Message[T]) ([]Message[T], error) {
	return []Message[T]{msg}, nil
}

// // Pipe is a handler which simply passes a message through without modification.
// func Pipe[T any](ctx context.Context, msg Message[T]) ([]Message[T], error) {
// 	return []Message[T]{msg}, nil
// }

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
	deser func([]byte) (T, error)
}

func NewDeserSource[T any](src ByteSource, deser DeserFunc[T]) DeserializationSource[T] {
	return DeserializationSource[T]{
		src:   src,
		deser: deser,
	}
}

func (ds DeserializationSource[T]) Recv(ctx context.Context) (Message[T], func(), error) {
	msg, ack, err := ds.src.Recv(ctx)
	if err != nil {
		return Message[T]{}, ack, err
	}
	val, err := ds.deser(msg.Value)

	ret := Message[T]{
		Key:        msg.Key,
		Value:      val,
		Topic:      msg.Topic,
		Attributes: msg.Attributes,
	}
	return ret, ack, err
}
