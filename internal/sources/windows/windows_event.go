package windows

import (
	"encoding/xml"
	"time"
)

// Event represents an Windows Event
// parsed from XML
type Event struct {
	XMLName   xml.Name     `xml:"Event"`
	System    *System      `xml:"System"`
	EventData []*EventData `xml:"EventData>Data"`
}

// FindEventData will lookup the eventData
// slice to find the first eventData entry
// that has a matching key
func (event *Event) FindEventData(key string) *EventData {
	for _, ed := range event.EventData {
		if ed.Key == key {
			return ed
		}
	}

	return nil
}

// EventData represents a `Data` element
// found in `EventData` of an Event XML
type EventData struct {
	Key   string `xml:"Name,attr"`
	Value string `xml:",chardata"`
}

// EventData represents a `Data` element
// found in `EventData` of an Event XML
type System struct {
	Provider struct {
		Name            string `xml:"Name,attr"`
		Guid            string `xml:"Guid,attr"`
		EventSourceName string `xml:"EventSourceName,attr"`
	} `xml:"Provider"`
	EventID     int    `xml:"EventID"`
	Version     int    `xml:"Version"`
	Level       int    `xml:"Level"`
	Task        int    `xml:"Task"`
	Opcode      int    `xml:"Opcode"`
	Keywords    string `xml:"Keywords"`
	TimeCreated struct {
		SystemTime time.Time `xml:"SystemTime,attr"`
	} `xml:"TimeCreated"`
	EventRecordID string `xml:"EventRecordID"`
	Correlation   string `xml:"Correlation"`
	Execution     struct {
		ProcessID string `xml:"ProcessID,attr"`
		ThreadID  string `xml:"ThreadID,attr"`
	} `xml:"Execution"`
	Channel  string `xml:"Channel"`
	Computer string `xml:"Computer"`
	Security struct {
		UserID string `xml:"UserID,attr"`
	} `xml:"Security"`
}
