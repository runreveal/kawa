package mqttDstkawad

import (
	"context"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/kawa/x/mqtt"
)

type MQTT struct {
	wrapped *mqtt.Destination
}

func NewMQTT(opts ...mqtt.OptFunc) (*MQTT, error) {
	dst, err := mqtt.NewDestination(opts...)
	if err != nil {
		return nil, err
	}
	return &MQTT{wrapped: dst}, nil
}

func (p *MQTT) Run(ctx context.Context) error {
	return p.wrapped.Run(ctx)
}

func (p *MQTT) Send(ctx context.Context, ack func(), msg ...kawa.Message[types.Event]) error {
	for _, m := range msg {
		err := p.wrapped.Send(ctx, ack, kawa.Message[[]byte]{Value: m.Value.RawLog})
		if err != nil {
			return err
		}
	}
	return nil
}
