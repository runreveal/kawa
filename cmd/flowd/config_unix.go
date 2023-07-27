//go:build aix || darwin || dragonfly || freebsd || (js && wasm) || linux || nacl || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

package main

import (
	"github.com/runreveal/flow"
	"github.com/runreveal/flow/internal/sources/journald"
	"github.com/runreveal/flow/internal/sources/syslog"
	"github.com/runreveal/flow/internal/types"
	"github.com/runreveal/lib/loader"
	"golang.org/x/exp/slog"
	// We could register and configure these in a separate package
	// using the init() function.
	// That would make it easy to "dynamically" enable and disable them at
	// compile time since it would simply be updating the import list.
)

func init() {
	loader.Register("syslog", func() loader.Builder[flow.Source[types.Event]] {
		return &SyslogConfig{}
	})
	loader.Register("journald", func() loader.Builder[flow.Source[types.Event]] {
		return &JournaldConfig{}
	})
}

type SyslogConfig struct {
	Addr string `json:"addr"`
}

func (c *SyslogConfig) Configure() (flow.Source[types.Event], error) {
	slog.Info("configuring syslog")
	return syslog.NewSyslogSource(syslog.SyslogCfg{
		Addr: c.Addr,
	}), nil
}

type JournaldConfig struct {
}

func (c *JournaldConfig) Configure() (flow.Source[types.Event], error) {
	slog.Info("configuring journald")
	return journald.New(), nil
}
