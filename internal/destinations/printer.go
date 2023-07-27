package destinations

import (
	"context"
	"fmt"
	"io"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/types"
)

type Printer struct {
	writer io.Writer
}

func NewPrinter(writer io.Writer) *Printer {
	return &Printer{writer: writer}
}

func (p *Printer) Send(ctx context.Context, ack func(), msg ...kawa.Message[types.Event]) error {
	for _, m := range msg {
		_, err := fmt.Fprintf(p.writer, "%s\n", m.Value.RawLog)
		if err != nil {
			return err
		}
	}
	if ack != nil {
		ack()
	}
	return nil
}
