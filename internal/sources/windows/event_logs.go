package windows

import (
	"context"
	"log"
	"sync"

	"github.com/runreveal/flow"
	"github.com/runreveal/flow/internal/types"
)

type EventViewer struct {
	msgC chan msgAck
}

type msgAck struct {
	msg flow.Message[types.Event]
	ack func()
}

func New() *EventViewer {
	return &EventViewer{
		msgC: make(chan msgAck),
	}
}

func (s *EventViewer) Run(ctx context.Context) error {
	return s.recvLoop(ctx)
}

var (
	errorsChan = make(chan error)
)

func (s *EventViewer) recvLoop(ctx context.Context) error {

	eventSubscription := &EventSubscription{
		Channel:         "Security",
		Query:           "*", //[EventData[Data[@Name='LogonType']='2'] and System[(EventID=4624)]]", // Successful interactive logon events
		SubscribeMethod: EvtSubscribeToFutureEvents,
		Errors:          errorsChan,
		Callback:        s.msgC,
	}

	if err := eventSubscription.Create(); err != nil {
		log.Fatalf("Failed to create event subscription: %s", err)
		return nil
	}

	defer eventSubscription.Close()

	// for err := range errorsChan {
	// 	log.Printf("Event subscription error: %s", err)
	// }

	// if err := eventSubscription.Close(); err != nil {
	// 	log.Fatalf("Encountered error while closing subscription: %s", err)
	// } else {
	// 	log.Println("Gracefully shutdown")
	// }

	var wg sync.WaitGroup

	for {
		wg.Wait()
	}

	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()
	select {
	case <-c:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (s *EventViewer) Recv(ctx context.Context) (flow.Message[types.Event], func(), error) {
	select {
	case <-ctx.Done():
		return flow.Message[types.Event]{}, nil, ctx.Err()
	case pass := <-s.msgC:
		return pass.msg, pass.ack, nil
	}
}

func eventCallback(event *Event) {
	targetDomain := event.FindEventData("TargetDomainName")
	targetUser := event.FindEventData("TargetUserName")
	logonType := event.FindEventData("LogonType")

	if targetUser != nil && targetDomain != nil && logonType != nil {
		log.Printf("User logon event received for [%s\\%s] - LogonType:%s ", targetDomain.Value, targetUser.Value, logonType.Value)
	}
}
