package multi

import (
	"context"
	"sync"

	"github.com/runreveal/kawa"
)

type msgAck[T any] struct {
	msg kawa.Message[T]
	ack func()
}

// MultiSource multiplexes multiple sources into one.  It was thrown together
// quickly and may have some performance issues.  It definitely needs some work
// on proper error handling, and concurrency issues on closing.
type MultiSource[T any] struct {
	wrapped []kawa.Source[T]
	msgAckC chan msgAck[T]
}

// TODO: options for ack behavior?
func NewMultiSource[T any](sources []kawa.Source[T]) MultiSource[T] {
	return MultiSource[T]{
		wrapped: sources,
		msgAckC: make(chan msgAck[T]),
	}
}

// Run assumes the wrapped sources are already running, it spawns a go-routine
// for each source being wrapped, and in a loop reads its Recv method, then
// makes that message available on the Recv method for the multi source.
// Sources will be "competing" to send events on the shared channel, which
// means that faster sources have the potential to "starve" slower ones.  At
// our current scale, this shouldn't be an issue.
func (ms MultiSource[T]) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	errc := make(chan error, len(ms.wrapped))

	for _, src := range ms.wrapped {
		wg.Add(1)
		go func(src kawa.Source[T]) {
			defer wg.Done()
			for {
				msg, ack, err := src.Recv(ctx)
				if err != nil {
					select {
					case errc <- err:
					case <-ctx.Done():
						return
					}
				}
				select {
				case ms.msgAckC <- msgAck[T]{msg: msg, ack: ack}:
				case <-ctx.Done():
					return
				}
			}
		}(src)
	}

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-errc:
	}
	wg.Wait()
	return err
}

func (ms MultiSource[T]) Recv(ctx context.Context) (kawa.Message[T], func(), error) {
	select {
	case ma := <-ms.msgAckC:
		return ma.msg, ma.ack, nil
	case <-ctx.Done():
		return kawa.Message[T]{}, nil, ctx.Err()
	}
}
