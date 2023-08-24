package scanner

import (
	"context"
	"io"
	"time"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/kawa/x/scanner"
)

type Scanner struct {
	wrapped *scanner.Scanner
}

func NewScanner(reader io.Reader) *Scanner {
	return &Scanner{
		wrapped: scanner.NewScanner(reader),
	}
}

func (s *Scanner) Run(ctx context.Context) error {
	return s.wrapped.Run(ctx)
}

func (s *Scanner) Recv(ctx context.Context) (kawa.Message[types.Event], func(), error) {
	msg, ack, err := s.wrapped.Recv(ctx)
	if err != nil {
		return kawa.Message[types.Event]{}, nil, err
	}
	return kawa.Message[types.Event]{
		Value: types.Event{
			Timestamp:  time.Now(),
			SourceType: "scanner",
			RawLog:     msg.Value,
		},
	}, ack, nil
}
