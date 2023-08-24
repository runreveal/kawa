package scanner

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/runreveal/kawa"
)

type Scanner struct {
	reader io.Reader
	msgC   chan kawa.MsgAck[[]byte]
}

func NewScanner(reader io.Reader) *Scanner {
	return &Scanner{
		reader: reader,
		msgC:   make(chan kawa.MsgAck[[]byte]),
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
		case s.msgC <- kawa.MsgAck[[]byte]{
			Msg: kawa.Message[[]byte]{
				Value: []byte(str),
			},
			Ack: func() {
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

func (s *Scanner) Recv(ctx context.Context) (kawa.Message[[]byte], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[[]byte]{}, nil, ctx.Err()
	case pass := <-s.msgC:
		return pass.Msg, pass.Ack, nil
	}
}
