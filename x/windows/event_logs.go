//go:build windows
// +build windows

package windows

import (
	"context"
	"time"

	"log/slog"

	"github.com/runreveal/kawa"
)

type Option func(*EventLogSource)

func WithChannel(channel string) Option {
	return func(s *EventLogSource) {
		s.Channel = channel
	}
}

func WithQuery(query string) Option {
	return func(s *EventLogSource) {
		s.Query = query
	}
}

type EventLogSource struct {
	msgC chan msgAck

	Channel string
	Query   string

	subscription *eventSubscription
}

type msgAck struct {
	msg kawa.Message[EventLog]
	ack func()
}

func NewEventLogSource(opts ...Option) *EventLogSource {
	ret := &EventLogSource{}
	for _, o := range opts {
		o(ret)
	}

	if ret.Query == "" {
		ret.Query = "*"
	}

	msgC := make(chan msgAck)
	errorsChan := make(chan error)

	eventSubscription := &eventSubscription{
		Channel:         ret.Channel,
		Query:           ret.Query, //[EventData[Data[@Name='LogonType']='2'] and System[(EventID=4624)]]", // Successful interactive logon events
		SubscribeMethod: evtSubscribeToFutureEvents,
		Errors:          errorsChan,
		Callback:        msgC,
	}

	ret.msgC = msgC
	ret.subscription = eventSubscription

	return ret
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

	for {
		select {
		case err := <-s.subscription.Errors:
			return err
		case <-ctx.Done():
			err := s.subscription.close()
			return err
		case <-time.After(60 * time.Second):
		}
	}
}

func (s *EventLogSource) Recv(ctx context.Context) (kawa.Message[EventLog], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[EventLog]{}, nil, ctx.Err()
	case pass := <-s.msgC:
		return pass.msg, pass.ack, nil
	}
}
