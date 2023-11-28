package queue

import (
	"context"
	"errors"

	"log/slog"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/kawa/x/multi"
	"github.com/runreveal/lib/await"
)

type Option func(*Queue)

type Source struct {
	Name   string
	Source kawa.Source[types.Event]
}

type Destination struct {
	Name        string
	Destination kawa.Destination[types.Event]
}

func WithSources(srcs map[string]Source) Option {
	return func(q *Queue) {
		q.Sources = srcs
	}
}

func WithDestinations(dsts map[string]Destination) Option {
	return func(q *Queue) {
		q.Destinations = dsts
	}
}

type Queue struct {
	Sources      map[string]Source
	Destinations map[string]Destination
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

	var srcs []kawa.Source[types.Event]
	for _, s := range q.Sources {
		if r, ok := s.Source.(await.Runner); ok {
			w.AddNamed(r, s.Name)
		}
		srcs = append(srcs, s.Source)
	}

	var dsts []kawa.Destination[types.Event]
	for _, d := range q.Destinations {
		if r, ok := d.Destination.(await.Runner); ok {
			w.AddNamed(r, d.Name)
		}
		dsts = append(dsts, d.Destination)
	}
	multiDst := multi.NewMultiDestination(dsts)
	multiSrc := multi.NewMultiSource(srcs)

	w.AddNamed(multiSrc, "multi-source")

	p, err := kawa.New(kawa.Config[types.Event, types.Event]{
		Source:      multiSrc,
		Destination: multiDst,
		Handler:     kawa.Pipe[types.Event](),
		// NOTE(alan): don't increase parallelism on this processor until we've
		// verified thread safety thread-safe story.
	}, kawa.Parallelism(1))
	if err != nil {
		return err
	}
	w.AddNamed(p, "processor")
	slog.Info("running queue")
	err = w.Run(ctx)
	slog.Error("stopping", "error", err)
	return err
}
