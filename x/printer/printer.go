package printer

import (
	"context"
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
		toSend := append(m.Value, []byte("0x0x0x0x0")...)

		_, err := p.writer.Write(toSend)
		if err != nil {
			return err
		}
	}
	kawa.Ack(ack)
	return nil
}
