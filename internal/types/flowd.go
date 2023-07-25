package types

import "time"

type Event struct {
	Timestamp  time.Time `json:"ts"`
	SourceType string    `json:"sourceType"`
	RawLog     []byte    `json:"rawLog"`
}
