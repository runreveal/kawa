package mqttSrckawad

import (
	"context"
	"time"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/kawa/x/mqtt"
)

type MQTT struct {
	wrapped *mqtt.Source
}

func NewMQTT(opts ...mqtt.OptFunc) (*MQTT, error) {
	src, err := mqtt.NewSource(opts...)
	if err != nil {
		return nil, err
	}
	return &MQTT{wrapped: src}, nil
}

func (s *MQTT) Run(ctx context.Context) error {
	return s.wrapped.Run(ctx)
}

func (s *MQTT) Recv(ctx context.Context) (kawa.Message[types.Event], func(), error) {
	msg, ack, err := s.wrapped.Recv(ctx)
	if err != nil {
		return kawa.Message[types.Event]{}, nil, ctx.Err()
	}

	eventMsg := kawa.Message[types.Event]{
		Key: msg.Key,
		Value: types.Event{
			Timestamp:  time.Now(),
			SourceType: "mqtt",
			RawLog:     msg.Value,
		}, Topic: msg.Topic,
		Attributes: msg.Attributes,
	}

	return eventMsg, ack, err
}
