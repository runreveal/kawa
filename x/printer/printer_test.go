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
				batchMessages := make([]kawa.Message[[]byte], len(batch))
				for i, s := range batch {
					batchMessages[i] = kawa.Message[[]byte]{
						Value: []byte(s),
					}
				}
				callCount := 0
				err := p.Send(ctx, func() { callCount++ }, batchMessages...)
				if err != nil {
					t.Errorf("Error sending batch #%d: %v", i+1, err)
				}
				if callCount != 1 {
					t.Errorf("ack function called %d times during batch #%d; want 1", callCount, i+1)
				}
			}

			if got := buf.String(); got != test.want {
				t.Errorf("wrote %q; want %q", got, test.want)
			}
		})
	}
}
