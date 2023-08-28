package main

import (
	"os"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/destinations/printer"
	"github.com/runreveal/kawa/cmd/kawad/internal/destinations/runreveal"
	s3kawad "github.com/runreveal/kawa/cmd/kawad/internal/destinations/s3"
	"github.com/runreveal/kawa/cmd/kawad/internal/sources/journald"
	"github.com/runreveal/kawa/cmd/kawad/internal/sources/scanner"
	"github.com/runreveal/kawa/cmd/kawad/internal/sources/syslog"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/kawa/x/s3"
	"github.com/runreveal/lib/loader"
	"golang.org/x/exp/slog"
	// We could register and configure these in a separate package
	// using the init() function.
	// That would make it easy to "dynamically" enable and disable them at
	// compile time since it would simply be updating the import list.
)

func init() {
	loader.Register("scanner", func() loader.Builder[kawa.Source[types.Event]] {
		return &ScannerConfig{}
	})
	loader.Register("syslog", func() loader.Builder[kawa.Source[types.Event]] {
		return &SyslogConfig{}
	})
	loader.Register("journald", func() loader.Builder[kawa.Source[types.Event]] {
		return &JournaldConfig{}
	})

	loader.Register("printer", func() loader.Builder[kawa.Destination[types.Event]] {
		return &PrinterConfig{}
	})
	loader.Register("s3", func() loader.Builder[kawa.Destination[types.Event]] {
		return &S3Config{}
	})
	loader.Register("runreveal", func() loader.Builder[kawa.Destination[types.Event]] {
		return &RunRevealConfig{}
	})
}

type ScannerConfig struct {
}

func (c *ScannerConfig) Configure() (kawa.Source[types.Event], error) {
	slog.Info("configuring scanner")
	return scanner.NewScanner(os.Stdin), nil
}

type SyslogConfig struct {
	Addr        string `json:"addr"`
	ContentType string `json:"contentType"`
}

func (c *SyslogConfig) Configure() (kawa.Source[types.Event], error) {
	slog.Info("configuring syslog")
	return syslog.NewSyslogSource(syslog.SyslogCfg{
		Addr:        c.Addr,
		ContentType: c.ContentType,
	}), nil
}

type PrinterConfig struct {
}

func (c *PrinterConfig) Configure() (kawa.Destination[types.Event], error) {
	slog.Info("configuring printer")
	return printer.NewPrinter(os.Stdout), nil
}

type RunRevealConfig struct {
	WebhookURL string `json:"webhookURL"`
}

func (c *RunRevealConfig) Configure() (kawa.Destination[types.Event], error) {
	slog.Info("configuring runreveal")
	return runreveal.New(
		runreveal.WithWebhookURL(c.WebhookURL),
	), nil
}

type S3Config struct {
	BucketName   string `json:"bucketName"`
	PathPrefix   string `json:"pathPrefix"`
	BucketRegion string `json:"bucketRegion"`

	CustomEndpoint  string `json:"customEndpoint"`
	AccessKeyID     string `json:"accessKeyID"`
	AccessSecretKey string `json:"accessSecretKey"`

	BatchSize int `json:"batchSize"`
}

func (c *S3Config) Configure() (kawa.Destination[types.Event], error) {
	slog.Info("configuring s3")
	return s3kawad.NewS3(
		s3.WithBucketName(c.BucketName),
		s3.WithBucketRegion(c.BucketRegion),
		s3.WithPathPrefix(c.PathPrefix),
		s3.WithCustomEndpoint(c.CustomEndpoint),
		s3.WithAccessKeyID(c.AccessKeyID),
		s3.WithAccessSecretKey(c.AccessSecretKey),
		s3.WithBatchSize(c.BatchSize),
	), nil
}

type JournaldConfig struct {
}

func (c *JournaldConfig) Configure() (kawa.Source[types.Event], error) {
	slog.Info("configuring journald")
	return journald.New(), nil
}
