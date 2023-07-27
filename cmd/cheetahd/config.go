package main

import (
	"os"

	"github.com/runreveal/chta"
	"github.com/runreveal/chta/internal/destinations"
	"github.com/runreveal/chta/internal/destinations/runreveal"
	"github.com/runreveal/chta/internal/sources"
	"github.com/runreveal/chta/internal/sources/journald"
	"github.com/runreveal/chta/internal/sources/syslog"
	"github.com/runreveal/chta/internal/types"
	"github.com/runreveal/lib/loader"
	"golang.org/x/exp/slog"
	// We could register and configure these in a separate package
	// using the init() function.
	// That would make it easy to "dynamically" enable and disable them at
	// compile time since it would simply be updating the import list.
)

func init() {
	loader.Register("scanner", func() loader.Builder[chta.Source[types.Event]] {
		return &ScannerConfig{}
	})
	loader.Register("syslog", func() loader.Builder[chta.Source[types.Event]] {
		return &SyslogConfig{}
	})
	loader.Register("journald", func() loader.Builder[chta.Source[types.Event]] {
		return &JournaldConfig{}
	})

	loader.Register("printer", func() loader.Builder[chta.Destination[types.Event]] {
		return &PrinterConfig{}
	})
	loader.Register("runreveal", func() loader.Builder[chta.Destination[types.Event]] {
		return &RunRevealConfig{}
	})
}

type ScannerConfig struct {
}

func (c *ScannerConfig) Configure() (chta.Source[types.Event], error) {
	slog.Info("configuring scanner")
	return sources.NewScanner(os.Stdin), nil
}

type SyslogConfig struct {
	Addr string `json:"addr"`
}

func (c *SyslogConfig) Configure() (chta.Source[types.Event], error) {
	slog.Info("configuring syslog")
	return syslog.NewSyslogSource(syslog.SyslogCfg{
		Addr: c.Addr,
	}), nil
}

type PrinterConfig struct {
}

func (c *PrinterConfig) Configure() (chta.Destination[types.Event], error) {
	slog.Info("configuring printer")
	return destinations.NewPrinter(os.Stdout), nil
}

type RunRevealConfig struct {
	WebhookURL string `json:"webhookURL"`
}

func (c *RunRevealConfig) Configure() (chta.Destination[types.Event], error) {
	slog.Info("configuring runreveal")
	return runreveal.New(
		runreveal.WithWebhookURL(c.WebhookURL),
	), nil
}

type JournaldConfig struct {
}

func (c *JournaldConfig) Configure() (chta.Source[types.Event], error) {
	slog.Info("configuring journald")
	return journald.New(), nil
}
