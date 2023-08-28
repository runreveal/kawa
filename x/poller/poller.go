package poller

import (
	"context"
	"errors"

	"github.com/runreveal/kawa"
)

type msgAck[T any] struct {
	msg kawa.Message[T]
	ack func()
}

type Poller[T any] interface {
	Poll(context.Context, int) ([]kawa.Message[T], func(), error)
}

type Source[T any] struct {
	msgChan   chan msgAck[T]
	batchSize int

	poller Poller[T]
}

func WithBatchSize(size int) func(*Opts) {
	return func(opts *Opts) {
		opts.BatchSize = size
	}
}

type Opts struct {
	BatchSize int
}

type Option func(*Opts)

func New[T any](p Poller[T], opts ...Option) *Source[T] {
	var cfg Opts
	for _, o := range opts {
		o(&cfg)
	}
	ret := &Source[T]{
		poller:    p,
		msgChan:   make(chan msgAck[T], cfg.BatchSize),
		batchSize: cfg.BatchSize,
	}
	return ret
}

func (s *Source[T]) Run(ctx context.Context) error {
	return s.recvLoop(ctx)
}

func (s *Source[T]) recvLoop(ctx context.Context) error {
	for {
		msgs, ack, err := s.poller.Poll(ctx, s.batchSize)
		if err != nil {
			return err
		}

		ackFn := ackLast(ack, len(msgs))

		for _, m := range msgs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.msgChan <- msgAck[T]{msg: m, ack: ackFn}:
			}
		}
	}
}

func (s *Source[T]) Recv(ctx context.Context) (kawa.Message[T], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[T]{}, nil, ctx.Err()
	case ma := <-s.msgChan:
		return ma.msg, ma.ack, errors.New("not implemented")
	}
}

// only call ack on last message acknowledgement
func ackLast(ack func(), num int) func() {
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
