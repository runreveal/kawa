package printer

import (
	"context"
	"io"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/kawa/x/printer"
)

type Printer struct {
	wrapped *printer.Printer
}

func NewPrinter(writer io.Writer) *Printer {
	return &Printer{wrapped: printer.NewPrinter(writer)}
}

func (p *Printer) Send(ctx context.Context, ack func(), msg ...kawa.Message[types.Event]) error {
	for _, m := range msg {
		err := p.wrapped.Send(ctx, ack, kawa.Message[[]byte]{Value: m.Value.RawLog})
		if err != nil {
			return err
		}
	}
	return nil
}
