package flow

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/runreveal/flow"
	"github.com/runreveal/flow/x/memory"
)

type BinString string

func (b *BinString) UnmarshalBinary(in []byte) error {
	*b = BinString(in)
	return nil
}

func (b BinString) MarshalBinary() ([]byte, error) {
	return []byte(b), nil
}

func TestSmokeHappyPath(t *testing.T) {
	schan := make(chan *BinString)
	src := memory.NewMemSource((<-chan *BinString)(schan))
	dst := memory.NewMemDestination[*BinString]((chan<- *BinString)(schan))
	var wg sync.WaitGroup
	wg.Add(10)

	go func() {
		for i := 0; i < 10; i++ {
			x := BinString(fmt.Sprintf("hi-%d", i))
			err := dst.Send(context.TODO(), nil, flow.Message[*BinString]{Value: &x})
			if err != nil {
				t.Log(err)
			}
		}
	}()
	go func(t *testing.T) {
		t.Helper()
		for i := 0; i < 10; i++ {
			msg, _, err := src.Recv(context.TODO())
			if err != nil {
				t.Errorf("%v", err)
			}
			fmt.Printf("%s\n", *msg.Value)
			wg.Done()
		}
	}(t)
	wg.Wait()
}

// TestWow shows an example of creating a source without having to consume a
// generic implementation of a source this of course assumes the source is
// aware of the type that you're looking to surface to the pipeline
func TestWow(t *testing.T) {
	schan := make(chan *BinString)
	src := NewMemSource((<-chan *BinString)(schan))
	dst := memory.NewMemDestination[*BinString]((chan<- *BinString)(schan))
	var wg sync.WaitGroup
	wg.Add(10)

	go func() {
		for i := 0; i < 10; i++ {
			x := BinString(fmt.Sprintf("wow-%d", i))
			err := dst.Send(context.TODO(), nil, flow.Message[*BinString]{Value: &x})
			if err != nil {
				t.Log(err)
			}
		}
	}()
	go func(t *testing.T) {
		t.Helper()
		for i := 0; i < 10; i++ {
			msg, _, err := src.Recv(context.TODO())
			if err != nil {
				t.Errorf("%v", err)
			}
			fmt.Printf("%s\n", *msg.Value)
			wg.Done()
		}
	}(t)
	wg.Wait()
}

type MemorySource struct {
	MsgC <-chan *BinString
}

func NewMemSource(in <-chan *BinString) MemorySource {
	return MemorySource{
		MsgC: in,
	}
}

func (ms MemorySource) Recv(ctx context.Context) (flow.Message[*BinString], func(), error) {
	select {
	case <-ctx.Done():
		return flow.Message[*BinString]{}, nil, ctx.Err()
	case v := <-ms.MsgC:
		return flow.Message[*BinString]{Value: v}, nil, nil
	}
}
