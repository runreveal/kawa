package queue

import (
	"context"
	"errors"

	"github.com/runreveal/chta"
	"github.com/runreveal/chta/internal/types"
	"github.com/runreveal/chta/x/multi"
	"github.com/runreveal/lib/await"
	"golang.org/x/exp/slog"
)

type Option func(*Queue)

func WithSources(srcs []chta.Source[types.Event]) Option {
	return func(q *Queue) {
		q.Sources = srcs
	}
}

func WithDestinations(dsts []chta.Destination[types.Event]) Option {
	return func(q *Queue) {
		q.Destinations = dsts
	}
}

type Queue struct {
	Sources      []chta.Source[types.Event]
	Destinations []chta.Destination[types.Event]
}

var (
	ErrNoSources      = errors.New("no sources configured")
	ErrNoDestinations = errors.New("no destinations configured")
)

func (q *Queue) Validate() error {
	if len(q.Sources) == 0 {
		return ErrNoSources
	}
	if len(q.Destinations) == 0 {
		return ErrNoDestinations
	}
	return nil
}

func New(opts ...Option) *Queue {
	var q Queue

	for _, opt := range opts {
		opt(&q)
	}

	return &q
}

func (q *Queue) Run(ctx context.Context) error {
	if err := q.Validate(); err != nil {
		return err
	}

	w := await.New(await.WithSignals)

	for _, s := range q.Sources {
		if r, ok := s.(interface {
			Run(context.Context) error
		}); ok {
			w.Add(r.Run)
		}
	}

	for _, s := range q.Destinations {
		if r, ok := s.(interface {
			Run(context.Context) error
		}); ok {
			w.Add(r.Run)
		}
	}

	multiDst := multi.NewMultiDestination(q.Destinations)
	// w.Add(multiDst.Run)

	multiSrc := multi.NewMultiSource(q.Sources)
	w.Add(multiSrc.Run)

	p, err := chta.New(chta.Config[types.Event, types.Event]{
		Source:      multiSrc,
		Destination: multiDst,
		Handler:     chta.Pipe[types.Event](),
		// NOTE(alan): don't increase parallelism on this processor until we've
		// verified thread safety thread-safe story.
	}, chta.Parallelism(1))
	if err != nil {
		return err
	}
	w.Add(p.Run)

	slog.Info("running queue")
	err = w.Run(ctx)
	slog.Error("await error", "error", err)
	return err
}
