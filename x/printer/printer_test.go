package printer

import (
	"context"
	"strings"
	"testing"

	"github.com/runreveal/kawa"
)

func TestPrinter(t *testing.T) {
	tests := []struct {
		name  string
		delim string
		sends [][]string
		want  string
	}{
		{
			name:  "NoSends",
			delim: "\n",
			sends: [][]string{},
			want:  "",
		},
		{
			name:  "SingleMessageDefaultDelimiter",
			delim: "\n",
			sends: [][]string{{"Hello, World"}},
			want:  "Hello, World\n",
		},
		{
			name:  "SingleMessageCustomDelimiter",
			delim: "|||",
			sends: [][]string{{"Hello, World"}},
			want:  "Hello, World|||",
		},
		{
			name:  "MultipleMessagesSingleBatch",
			delim: "\n",
			sends: [][]string{{"abc", "def"}},
			want:  "abc\ndef\n",
		},
		{
			name:  "MultipleBatchesWithSingleMessage",
			delim: "\n",
			sends: [][]string{{"abc"}, {"def"}},
			want:  "abc\ndef\n",
		},
		{
			name:  "MultipleBatchesWithMultipleMessages",
			delim: "\n",
			sends: [][]string{{"abc", "def"}, {"ghi", "jkl"}},
			want:  "abc\ndef\nghi\njkl\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			var options []Option
			if test.delim != "\n" {
				options = append(options, WithDelim([]byte(test.delim)))
			}
			buf := new(strings.Builder)
			p := NewPrinter(buf, options...)

			for i, batch := range test.sends {
				for j, s := range batch {
					var ack func()
					if j == len(batch)-1 {
						callCount := 0
						ack = func() { callCount++ }
						err := p.Send(ctx, ack, kawa.Message[[]byte]{Value: []byte(s)})
						if err != nil {
							t.Errorf("Error sending message #%d in batch #%d: %v", j+1, i+1, err)
						}
						if callCount != 1 {
							t.Errorf("ack function called %d times for last message in batch #%d; want 1", callCount, i+1)
						}
					} else {
						err := p.Send(ctx, nil, kawa.Message[[]byte]{Value: []byte(s)})
						if err != nil {
							t.Errorf("Error sending message #%d in batch #%d: %v", j+1, i+1, err)
						}
					}
				}
			}

			if got := buf.String(); got != test.want {
				t.Errorf("wrote %q; want %q", got, test.want)
			}
		})
	}
}
