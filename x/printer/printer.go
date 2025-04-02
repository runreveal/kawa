package printer

import (
	"context"
	"io"
	"slices"

	"github.com/runreveal/kawa"
)

// A Printer is a [kawa.Destination] that serializes []byte messages
// to an [io.Writer].
type Printer struct {
	delim []byte

	mu     chan struct{} // send for lock, receive for unlock
	buf    []byte
	writer io.Writer
}

// Option is an optional argument to [NewPrinter].
type Option func(*Printer)

// WithDelim changes the sequence written after every message
// from the default newline ("\n") to the given byte sequence.
func WithDelim(delim []byte) Option {
	return func(p *Printer) {
		// Defensive copy.
		p.delim = slices.Clone(delim)
	}
}

// NewPrinter returns a new [Printer] that writes to the given writer.
func NewPrinter(writer io.Writer, opts ...Option) *Printer {
	ret := &Printer{
		writer: writer,
		delim:  []byte("\n"),
		mu:     make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

// Send implements [kawa.Destination] by writing each message value followed by the printer's delimiter.
// A call to Send will call Write at most once on the underlying writer.
func (p *Printer) Send(ctx context.Context, ack func(), msg ...kawa.Message[[]byte]) error {
	n := len(p.delim) * len(msg)
	for _, m := range msg {
		n += len(m.Value)
	}

	select {
	case p.mu <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.buf = slices.Grow(p.buf[:0], n)
	for _, m := range msg {
		p.buf = append(p.buf, m.Value...)
		p.buf = append(p.buf, p.delim...)
	}
	_, err := p.writer.Write(p.buf)
	<-p.mu // Release mutex as soon as possible after Write.

	if err == nil {
		kawa.Ack(ack)
	}
	return err
}
