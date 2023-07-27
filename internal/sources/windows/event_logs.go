package windows

import (
	"context"
	"fmt"
	"time"

	"github.com/runreveal/flow"
	"github.com/runreveal/flow/internal/types"
	"golang.org/x/exp/slog"
)

type EventLogCfg struct {
	Channel string `json:"channel"`
	Query   string `json:"query"`
}

type EventLogSource struct {
	cfg          EventLogCfg
	msgC         chan msgAck
	subscription *eventSubscription
}

type msgAck struct {
	msg flow.Message[types.Event]
	ack func()
}

func NewEventLogSource(cfg EventLogCfg) *EventLogSource {
	queryStr := "*"
	msgC := make(chan msgAck)
	errorsChan := make(chan error)

	if cfg.Query != "" {
		queryStr = cfg.Query
	}

	eventSubscription := &eventSubscription{
		Channel:         cfg.Channel,
		Query:           queryStr, //[EventData[Data[@Name='LogonType']='2'] and System[(EventID=4624)]]", // Successful interactive logon events
		SubscribeMethod: EvtSubscribeToFutureEvents,
		Errors:          errorsChan,
		Callback:        msgC,
	}

	return &EventLogSource{
		msgC:         msgC,
		subscription: eventSubscription,
		cfg:          cfg,
	}
}

func (s *EventLogSource) Run(ctx context.Context) error {
	return s.recvLoop(ctx)
}

func (s *EventLogSource) recvLoop(ctx context.Context) error {

	if err := s.subscription.create(); err != nil {
		slog.Error("Failed to create event subscription: %s", err)
		return nil
	}
	defer s.subscription.close()
	defer close(s.subscription.Errors)

	go func() {
		for err := range s.subscription.Errors {
			slog.Error(fmt.Sprintf("%+v", err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			err := s.subscription.close()
			close(s.subscription.Errors)
			return err
		case <-time.After(60 * time.Second):
		}
	}
}

func (s *EventLogSource) Recv(ctx context.Context) (flow.Message[types.Event], func(), error) {
	select {
	case <-ctx.Done():
		return flow.Message[types.Event]{}, nil, ctx.Err()
	case pass := <-s.msgC:
		return pass.msg, pass.ack, nil
	}
}
