package scanner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/runreveal/kawa"
)

type Scanner struct {
	scanner *bufio.Scanner
	msgC    chan kawa.MsgAck[[]byte]
	delim   []byte
}

func WithDelim(delim []byte) func(*Scanner) {
	return func(s *Scanner) {
		s.delim = delim
	}
}

func NewScanner(reader io.Reader, opts ...func(*Scanner)) *Scanner {
	ret := &Scanner{
		scanner: bufio.NewScanner(reader),
		msgC:    make(chan kawa.MsgAck[[]byte]),
		delim:   []byte("\n"),
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (s *Scanner) Run(ctx context.Context) error {
	return s.recvLoop(ctx)
}

func (s *Scanner) recvLoop(ctx context.Context) error {
	var wg sync.WaitGroup
	s.scanner.Split(delimFunc(s.delim))
	for s.scanner.Scan() {
		bts := s.scanner.Bytes()
		val := make([]byte, len(bts))
		copy(val, bts)
		wg.Add(1)
		select {
		case s.msgC <- kawa.MsgAck[[]byte]{
			Msg: kawa.Message[[]byte]{Value: val},
			Ack: func() {
				wg.Done()
			},
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := s.scanner.Err(); err != nil {
		return fmt.Errorf("scanning: %+w", err)
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

func delimFunc(delim []byte) func(data []byte, atEOF bool) (advance int, token []byte, err error) {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.Index(data, delim); i >= 0 {
			return i + len(delim), data[0:i], nil
		}
		// If we're at EOF, we have a final, non-terminated line. Return it.
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
}
