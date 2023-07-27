package sources

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/runreveal/chta"
	"github.com/runreveal/chta/internal/types"
)

type Scanner struct {
	reader io.Reader
	msgC   chan msgAck
}

type msgAck struct {
	msg chta.Message[types.Event]
	ack func()
}

func NewScanner(reader io.Reader) *Scanner {
	return &Scanner{
		reader: reader,
		msgC:   make(chan msgAck),
	}
}

func (s *Scanner) Run(ctx context.Context) error {
	return s.recvLoop(ctx)
}

func (s *Scanner) recvLoop(ctx context.Context) error {
	scanner := bufio.NewScanner(s.reader)
	var wg sync.WaitGroup

	for scanner.Scan() {
		str := scanner.Text()
		// NOTE: what does it mean to acknolwedge a message was successfully
		// processed in the context of a file source?  There isn't really an
		// upstream to communicate with when consuming an io.Reader.
		wg.Add(1)
		select {
		case s.msgC <- msgAck{
			msg: chta.Message[types.Event]{
				Value: types.Event{
					Timestamp:  time.Now(),
					SourceType: "reader",
					RawLog:     []byte(str),
				},
			},
			ack: func() {
				wg.Done()
			},
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()
	select {
	case <-c:
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning: %+w", err)
	}

	return nil
}

func (s *Scanner) Recv(ctx context.Context) (chta.Message[types.Event], func(), error) {
	select {
	case <-ctx.Done():
		return chta.Message[types.Event]{}, nil, ctx.Err()
	case pass := <-s.msgC:
		return pass.msg, pass.ack, nil
	}
}
