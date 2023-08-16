package sources

import (
	"context"
	"sync"
	"time"

	"github.com/hpcloud/tail"
	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/types"
)

type Tail struct {
	file string
	msgC chan msgAck
}

func NewTail(file string) *Tail {
	return &Tail{
		file: file,
		msgC: make(chan msgAck),
	}
}

func (t *Tail) Run(ctx context.Context) error {
	return t.recvLoop(ctx)
}

func (t *Tail) recvLoop(ctx context.Context) error {
	var wg sync.WaitGroup
	tailer, err := tail.TailFile(t.file, tail.Config{Follow: true})
	if err != nil {
		return err
	}

	for line := range tailer.Lines {
		wg.Add(1)
		select {
		case t.msgC <- msgAck{
			msg: kawa.Message[types.Event]{
				Value: types.Event{
					Timestamp:  time.Now(),
					SourceType: "tail",
					RawLog:     []byte(line.Text),
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
	return nil
}

func (s *Tail) Recv(ctx context.Context) (kawa.Message[types.Event], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[types.Event]{}, nil, ctx.Err()
	case pass := <-s.msgC:
		return pass.msg, pass.ack, nil
	}
}
