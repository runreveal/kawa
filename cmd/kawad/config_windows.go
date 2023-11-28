//go:build windows
// +build windows

package main

import (
	"log/slog"

	"github.com/runreveal/kawa"
	windowskawad "github.com/runreveal/kawa/cmd/kawad/internal/sources/windows"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/kawa/x/windows"
	"github.com/runreveal/lib/loader"
)

func init() {
	loader.Register("eventlog", func() loader.Builder[kawa.Source[types.Event]] {
		return &EventLogConfig{}
	})
}

type EventLogConfig struct {
	Channel string `json:"channel"`
	Query   string `json:"query"`
}

func (c *EventLogConfig) Configure() (kawa.Source[types.Event], error) {
	slog.Info("configuring windows event log")
	return windowskawad.NewEventLog(windows.WithChannel(c.Channel), windows.WithQuery(c.Query)), nil
}
