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

	scanner.Split(ScanDelim([]byte("0x0x0x0x0")))

	count := 0
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
		count++
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

func ScanDelim(delim []byte) func(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
