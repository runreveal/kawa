package flow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/runreveal/flow"
	"github.com/runreveal/flow/x/memory"
)

type BinString string

func (bs *BinString) MarshalBinary() ([]byte, error) {
	return []byte(*bs), nil
}

func (bs *BinString) UnmarshalBinary(bts []byte) error {
	*bs = BinString(bts[:])
	return nil
}

func TestProcessor(t *testing.T) {
	inC, outC := make(chan *BinString), make(chan *BinString)
	memSrc := memory.MemorySource[*BinString]{
		MsgC: inC,
	}
	memDst := memory.MemoryDestination[*BinString]{
		MsgC: outC,
	}

	countMessages := func(c context.Context, m flow.Message[*BinString]) ([]flow.Message[*BinString], error) {
		return []flow.Message[*BinString]{m}, nil
	}

	p, _ := flow.New[*BinString, *BinString](flow.Config[*BinString, *BinString]{
		Source:      memSrc,
		Destination: memDst,
		Handler:     countMessages,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := p.Run(ctx)
		fmt.Println(err)
	}()

	for i := 0; i < 10; i++ {
		bs := BinString("hi")
		inC <- &bs
		fmt.Println(<-outC)
	}

}
