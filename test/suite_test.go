package kawa_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/runreveal/kawa"
	"github.com/runreveal/lib/await"
	"github.com/stretchr/testify/assert"
)

func SuiteTest(t *testing.T, src kawa.Source[[]byte], dst kawa.Destination[[]byte]) {
	wait := await.New()
	want := make([][]byte, 25)
	seen := make([]bool, 25)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range want {
		want[i] = make([]byte, 20)
		_, err := rng.Read(want[i])
		assert.NoError(t, err)
	}

	if runnner, ok := src.(interface {
		Run(context.Context) error
	}); ok {
		wait.Add(runnner.Run)
	}
	if runnner, ok := dst.(interface {
		Run(context.Context) error
	}); ok {
		wait.Add(runnner.Run)
	}

	// stdoutDumper := hex.Dumper(os.Stdout)
	// defer stdoutDumper.Close()
	// for _, line := range want {
	// 	stdoutDumper.Write([]byte(line))
	// }

	wait.Add(func(ctx context.Context) error {
		count := 0
		for {
			msg, ack, err := src.Recv(ctx)
			if !errors.Is(err, context.Canceled) {
				assert.NoError(t, err)
			}
			if ack != nil {
				ack()
			}
			// fmt.Println("received:")
			// stdoutDumper.Write([]byte(msg.Value))
			// fmt.Printf("\n")
			mark(t, msg.Value, want, seen)
			count++
			if count == len(want) {
				break
			}
		}
		return nil
	})

	wait.Add(func(ctx context.Context) error {
		for i := range want {
			toSend := make([]byte, len(want[i]))
			copy(toSend, want[i])
			// fmt.Println("sent:")
			// stdoutDumper.Write(toSend)
			// fmt.Printf("\n")
			err := dst.Send(ctx, nil, kawa.Message[[]byte]{Value: toSend})
			if !errors.Is(err, context.Canceled) {
				assert.NoError(t, err)
			}
		}
		// Wait until the source exits.
		<-ctx.Done()
		return nil
	})
	ctx, cncl := context.WithTimeout(context.Background(), 5*time.Second)
	defer cncl()
	err := wait.Run(ctx)
	assert.NoError(t, err)
	for i := range seen {
		assert.True(t, seen[i], "we should have seen all messages")
	}
}

func mark(t *testing.T, actual []byte, sent [][]byte, seen []bool) {
	t.Helper()
	for i, want := range sent {
		if bytes.Equal(actual, want) {
			assert.False(t, seen[i], "we shouldn't see duplicates")
			seen[i] = true
			return
		}
	}
}
