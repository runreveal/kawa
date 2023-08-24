package printer

import (
	"context"
	"fmt"
	"io"

	"github.com/runreveal/kawa"
)

type Printer struct {
	writer io.Writer
}

func NewPrinter(writer io.Writer) *Printer {
	return &Printer{writer: writer}
}

func (p *Printer) Send(ctx context.Context, ack func(), msg ...kawa.Message[[]byte]) error {
	for _, m := range msg {
		_, err := fmt.Fprintf(p.writer, "%s\n", m.Value)
		if err != nil {
			return err
		}
	}
	kawa.Ack(ack)
	return nil
}
