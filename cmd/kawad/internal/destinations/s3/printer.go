package s3kawad

import (
	"context"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/kawa/x/s3"
)

type S3 struct {
	wrapped *s3.S3
}

func NewS3(opts ...s3.Option) *S3 {
	return &S3{wrapped: s3.New(opts...)}
}

func (p *S3) Send(ctx context.Context, ack func(), msg ...kawa.Message[types.Event]) error {
	for _, m := range msg {
		err := p.wrapped.Send(ctx, ack, kawa.Message[[]byte]{Value: m.Value.RawLog})
		if err != nil {
			return err
		}
	}
	return nil
}
