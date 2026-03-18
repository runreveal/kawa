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
	// Attributes are used to pass metadata as key-value pairs from a source
	// implementation through to a consumer. Use NewAttributes(), Get(), and
	// Set() to work with attributes.
	Attributes Attributes
}

// attrThreshold is the number of entries at which Attributes promotes from a
// flat slice to a map. Below this, linear scan on contiguous memory wins due
// to CPU cache locality and lower allocation overhead.
const attrThreshold = 16

// attr is a single key-value pair stored in the slice representation.
type attr struct {
	key   string
	value any
}

// Attributes is a key-value store for passing metadata from a source
// implementation through to a consumer. Keys are strings and values can be
// any type. The zero value is ready to use.
//
// Internally uses a flat slice for small entry counts (<=16) for cache-friendly
// access and cheap copies, and automatically promotes to a map when the entry
// count exceeds the threshold.
type Attributes struct {
	kvs []attr         // used when len <= attrThreshold
	m   map[string]any // used when len > attrThreshold
}

// NewAttributes creates an Attributes pre-allocated for the given number of
// key-value pairs. The zero value of Attributes is also valid and requires
// no constructor.
func NewAttributes(size ...int) Attributes {
	n := 0
	if len(size) > 0 {
		n = size[0]
	}
	if n < 0 {
		n = 0
	}
	if n > attrThreshold {
		return Attributes{m: make(map[string]any, n)}
	}
	return Attributes{kvs: make([]attr, 0, n)}
}

func (a Attributes) isMap() bool {
	return a.m != nil
}

// promote converts the slice representation to a map.
func (a Attributes) promote() Attributes {
	m := make(map[string]any, len(a.kvs))
	for _, kv := range a.kvs {
		m[kv.key] = kv.value
	}
	return Attributes{m: m}
}

func (a Attributes) appendTo(dst []attr) []attr {
	if a.isMap() {
		for k, v := range a.m {
			dst = append(dst, attr{key: k, value: v})
		}
		return dst
	}
	return append(dst, a.kvs...)
}

// Set returns a copy of the Attributes with key set to value, leaving the
// original unmodified.
func (a Attributes) Set(key string, value any) Attributes {
	if a.isMap() {
		n := make(map[string]any, len(a.m)+1)
		for k, v := range a.m {
			n[k] = v
		}
		n[key] = value
		return Attributes{m: n}
	}

	for i, kv := range a.kvs {
		if kv.key == key {
			n := make([]attr, len(a.kvs))
			copy(n, a.kvs)
			n[i].value = value
			return Attributes{kvs: n}
		}
	}

	// Adding a new key would cross the threshold — promote to map.
	if len(a.kvs)+1 > attrThreshold {
		r := a.promote()
		r.m[key] = value
		return r
	}

	n := make([]attr, len(a.kvs), len(a.kvs)+1)
	copy(n, a.kvs)
	n = append(n, attr{key: key, value: value})
	return Attributes{kvs: n}
}

// Delete returns a copy of the Attributes with the given key removed. If the
// key does not exist, the original is returned. When deletion brings a map
// representation back to attrThreshold or below, it demotes to a flat slice.
func (a Attributes) Delete(key string) Attributes {
	if a.isMap() {
		if _, ok := a.m[key]; !ok {
			return a
		}
		if len(a.m)-1 <= attrThreshold {
			kvs := make([]attr, 0, len(a.m)-1)
			for k, v := range a.m {
				if k != key {
					kvs = append(kvs, attr{key: k, value: v})
				}
			}
			return Attributes{kvs: kvs}
		}
		n := make(map[string]any, len(a.m)-1)
		for k, v := range a.m {
			if k != key {
				n[k] = v
			}
		}
		return Attributes{m: n}
	}

	idx := -1
	for i, kv := range a.kvs {
		if kv.key == key {
			idx = i
			break
		}
	}
	if idx < 0 {
		return a
	}
	n := make([]attr, 0, len(a.kvs)-1)
	n = append(n, a.kvs[:idx]...)
	n = append(n, a.kvs[idx+1:]...)
	return Attributes{kvs: n}
}

// Get retrieves a value by key. The second return value reports whether the
// key was found.
func (a Attributes) Get(key string) (any, bool) {
	if a.isMap() {
		v, ok := a.m[key]
		return v, ok
	}
	for _, kv := range a.kvs {
		if kv.key == key {
			return kv.value, true
		}
	}
	return nil, false
}

// GetAs is a typed helper that retrieves a value by key and asserts its type.
// Returns the zero value of T and false if the key is missing or the type
// does not match.
func GetAs[T any](a Attributes, key string) (T, bool) {
	v, ok := a.Get(key)
	if !ok {
		var zero T
		return zero, false
	}
	t, ok := v.(T)
	return t, ok
}

// Merge returns a new Attributes containing all key-value pairs from both a
// and other. Keys in other take precedence over keys in a.
func (a Attributes) Merge(other Attributes) Attributes {
	aLen := a.Len()
	oLen := other.Len()
	if aLen == 0 {
		return other
	}
	if oLen == 0 {
		return a
	}

	// If combined size will likely exceed threshold, merge via map.
	if aLen+oLen > attrThreshold {
		n := make(map[string]any, aLen+oLen)
		a.rangeFunc(func(k string, v any) { n[k] = v })
		other.rangeFunc(func(k string, v any) { n[k] = v })
		return Attributes{m: n}
	}

	// Both small — merge via slice.
	n := a.appendTo(make([]attr, 0, aLen+oLen))
	otherKVs := other.appendTo(make([]attr, 0, oLen))
	for _, okv := range otherKVs {
		found := false
		for i, nkv := range n {
			if nkv.key == okv.key {
				n[i].value = okv.value
				found = true
				break
			}
		}
		if !found {
			n = append(n, okv)
		}
	}
	return Attributes{kvs: n}
}

// Len returns the number of key-value pairs.
func (a Attributes) Len() int {
	if a.isMap() {
		return len(a.m)
	}
	return len(a.kvs)
}

// rangeFunc iterates over all key-value pairs, calling fn for each.
func (a Attributes) rangeFunc(fn func(string, any)) {
	if a.isMap() {
		for k, v := range a.m {
			fn(k, v)
		}
		return
	}
	for _, kv := range a.kvs {
		fn(kv.key, kv.value)
	}
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

// MsgAck is a utility type which is used to pass a message and it's
// corresponding ack function through a channel internal to a source or
// destination
type MsgAck[T any] struct {
	Msg Message[T]
	Ack func()
}

// Ack is a convenience function for calling the ack function after checking if
// it's nil.
func Ack(ack func()) {
	if ack != nil {
		ack()
	}
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
