package windows

import (
	"encoding/xml"
	"fmt"
	"syscall"
	"unsafe"

	"github.com/runreveal/flow"
	"github.com/runreveal/flow/internal/types"
	"golang.org/x/sys/windows"
)

// Credit to https://github.com/KatelynHaworth who created this code for reading the event log.
// Code was changed to work with flowd.

const (
	// EvtSubscribeToFutureEvents instructs the
	// subscription to only receive events that occur
	// after the subscription has been made
	EvtSubscribeToFutureEvents = 1

	// EvtSubscribeStartAtOldestRecord instructs the
	// subscription to receive all events (past and future)
	// that match the query
	EvtSubscribeStartAtOldestRecord = 2

	// evtSubscribeActionError defines a action
	// code that may be received by the winAPICallback.
	// ActionError defines that an internal error occurred
	// while obtaining an event for the callback
	evtSubscribeActionError = 0

	// evtSubscribeActionDeliver defines a action
	// code that may be received by the winAPICallback.
	// ActionDeliver defines that the internal API was
	// successful in obtaining an event that matched
	// the subscription query
	evtSubscribeActionDeliver = 1

	// evtRenderEventXML instructs procEvtRender
	// to render the event details as a XML string
	evtRenderEventXML = 1
)

var (
	modwevtapi = windows.NewLazySystemDLL("wevtapi.dll")

	procEvtSubscribe = modwevtapi.NewProc("EvtSubscribe")
	procEvtRender    = modwevtapi.NewProc("EvtRender")
	procEvtClose     = modwevtapi.NewProc("EvtClose")
)

// EventCallback defines the function
// layout required to receive events
type EventCallback func(event *Event)

// EventSubscription is a subscription to
// Windows Events, it defines details about the
// subscription including the channel and query
type EventSubscription struct {
	Channel         string
	Query           string
	SubscribeMethod int
	Errors          chan error
	Callback        chan msgAck

	winAPIHandle windows.Handle
}

// Create will setup an event subscription in the
// windows kernel with the provided channel and
// event query
func (evtSub *EventSubscription) Create() error {
	if evtSub.winAPIHandle != 0 {
		return fmt.Errorf("windows_events: subscription already created in kernel")
	}

	winChannel, err := windows.UTF16PtrFromString(evtSub.Channel)
	if err != nil {
		return fmt.Errorf("windows_events: bad channel name: %s", err)
	}

	winQuery, err := windows.UTF16PtrFromString(evtSub.Query)
	if err != nil {
		return fmt.Errorf("windows_events: bad query string: %s", err)
	}

	handle, _, err := procEvtSubscribe.Call(
		0,
		0,
		uintptr(unsafe.Pointer(winChannel)),
		uintptr(unsafe.Pointer(winQuery)),
		0,
		0,
		syscall.NewCallback(evtSub.winAPICallback),
		uintptr(evtSub.SubscribeMethod),
	)

	if handle == 0 {
		return fmt.Errorf("windows_events: failed to subscribe to events: %s", err)
	}

	evtSub.winAPIHandle = windows.Handle(handle)
	return nil
}

// Close tells the windows kernel to let go
// of the event subscription handle as we
// are now done with it
func (evtSub *EventSubscription) Close() error {
	if evtSub.winAPIHandle == 0 {
		return fmt.Errorf("windows_events: no subscription to close")
	}

	if returnCode, _, err := procEvtClose.Call(uintptr(evtSub.winAPIHandle)); returnCode == 0 {
		return fmt.Errorf("windows_events: encountered error while closing event handle: %s", err)
	}

	evtSub.winAPIHandle = 0
	return nil
}

// winAPICallback receives the callback from the windows
// kernel when an event matching the query and channel is
// received. It will query the kernel to get the event rendered
// as a XML string, the XML string is then unmarshaled to an
// `Event` and the custom callback invoked
func (evtSub *EventSubscription) winAPICallback(action, userContext, event uintptr) uintptr {
	switch action {
	case evtSubscribeActionError:
		evtSub.Errors <- fmt.Errorf("windows_events: encountered error during callback: Win32 Error %x", uint16(event))

	case evtSubscribeActionDeliver:
		renderSpace := make([]uint16, 4096)
		bufferUsed := uint16(0)
		propertyCount := uint16(0)

		returnCode, _, err := procEvtRender.Call(0, event, evtRenderEventXML, 4096, uintptr(unsafe.Pointer(&renderSpace[0])), uintptr(unsafe.Pointer(&bufferUsed)), uintptr(unsafe.Pointer(&propertyCount)))

		if returnCode == 0 {
			evtSub.Errors <- fmt.Errorf("windows_event: failed to render event data: %s", err)
		} else {
			dataParsed := new(Event)
			err := xml.Unmarshal([]byte(windows.UTF16ToString(renderSpace)), dataParsed)

			if err != nil {
				evtSub.Errors <- fmt.Errorf("windows_event: failed to unmarshal event xml: %s", err)
			} else {
				// take dataParsed and convert back to json object for sending to server
				msg := msgAck{
					msg: flow.Message[types.Event]{
						Value: types.Event{
							Timestamp:  dataParsed.System.TimeCreated.SystemTime,
							SourceType: dataParsed.System.Channel,
							RawLog:     []byte(windows.UTF16ToString(renderSpace)),
						},
					},
					ack: nil,
				}
				evtSub.Callback <- msg
			}
		}

	default:
		evtSub.Errors <- fmt.Errorf("windows_events: encountered error during callback: unsupported action code %x", uint16(action))
	}

	return 0
}
