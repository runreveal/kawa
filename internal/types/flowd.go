package types

import "time"

type Event struct {
	Timestamp   time.Time `json:"ts"`
	SourceType  string    `json:"sourceType"`
	ContentType string    `json:"contentType"`
	RawLog      []byte    `json:"rawLog"`
}
