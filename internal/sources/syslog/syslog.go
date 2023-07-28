package syslog

import (
	"context"
	"fmt"
	"time"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/types"
	"golang.org/x/exp/slog"
	"gopkg.in/mcuadros/go-syslog.v2"
)

type SyslogCfg struct {
	Addr        string `json:"addr"`
	ContentType string `json:"contentType"`
}

type SyslogSource struct {
	cfg          SyslogCfg
	server       *syslog.Server
	syslogPartsC syslog.LogPartsChannel
}

func NewSyslogSource(cfg SyslogCfg) *SyslogSource {
	server := syslog.NewServer()
	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)
	server.SetFormat(syslog.RFC3164)
	server.SetHandler(handler)
	return &SyslogSource{
		cfg:          cfg,
		server:       server,
		syslogPartsC: channel,
	}
}

func (s *SyslogSource) Run(ctx context.Context) error {
	slog.Info(fmt.Sprintf("starting syslog server on socket %s", s.cfg.Addr))
	err := s.server.ListenUDP(s.cfg.Addr)
	if err != nil {
		return err
	}
	err = s.server.Boot()
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		s.server.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err := s.server.Kill()
		return err
	case <-done:
	}
	return nil
}

func (s *SyslogSource) Recv(ctx context.Context) (kawa.Message[types.Event], func(), error) {
	select {
	case logParts := <-s.syslogPartsC:
		if content, ok := logParts["content"]; ok {
			rawLog := []byte(content.(string))

			ts := time.Now().UTC()
			if timestamp, ok := logParts["timestamp"]; ok {
				if ts, ok = timestamp.(time.Time); !ok {
					ts = time.Now().UTC()
				}
			}

			msg := kawa.Message[types.Event]{
				Value: types.Event{
					Timestamp:   ts,
					SourceType:  "syslog",
					RawLog:      rawLog,
					ContentType: s.cfg.ContentType,
				},
			}
			return msg, nil, nil
		} else {
			fmt.Println("warn: found syslog without 'content' key")
		}
	case <-ctx.Done():
		return kawa.Message[types.Event]{}, nil, ctx.Err()
	}
	panic("unreachable!")
}
