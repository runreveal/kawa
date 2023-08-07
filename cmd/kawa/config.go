package main

import (
	"os"

	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/internal/destinations"
	"github.com/runreveal/kawa/internal/destinations/mqtt"
	"github.com/runreveal/kawa/internal/destinations/runreveal"
	s3 "github.com/runreveal/kawa/internal/destinations/s3"
	"github.com/runreveal/kawa/internal/sources"
	"github.com/runreveal/kawa/internal/sources/journald"
	mqttsrc "github.com/runreveal/kawa/internal/sources/mqtt"
	"github.com/runreveal/kawa/internal/sources/syslog"
	"github.com/runreveal/kawa/internal/types"
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
	loader.Register("mqtt", func() loader.Builder[kawa.Source[types.Event]] {
		return &MQTTSrcConfig{}
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
	loader.Register("mqtt", func() loader.Builder[kawa.Destination[types.Event]] {
		return &MQTTConfig{}
	})
}

type ScannerConfig struct {
}

func (c *ScannerConfig) Configure() (kawa.Source[types.Event], error) {
	slog.Info("configuring scanner")
	return sources.NewScanner(os.Stdin), nil
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
	return destinations.NewPrinter(os.Stdout), nil
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

type MQTTConfig struct {
	Broker   string `json:"broker"`
	ClientID string `json:"clientID"`
	Topic    string `json:"topic"`

	UserName string `json:"userName"`
	Password string `json:"password"`

	QOS       byte `json:"qos"`
	Retained  bool `json:"retained"`
	BatchSize int  `json:"batchSize"`
}

func (c *MQTTConfig) Configure() (kawa.Destination[types.Event], error) {
	slog.Info("configuring mqtt")
	return mqtt.New(
		mqtt.WithBroker(c.Broker),
		mqtt.WithClientID(c.ClientID),
		mqtt.WithQOS(c.QOS),
		mqtt.WithBatchSize(c.BatchSize),
		mqtt.WithTopic(c.Topic),
		mqtt.WithRetained(c.Retained),
		mqtt.WithUserName(c.UserName),
		mqtt.WithPassword(c.Password),
	), nil
}

type MQTTSrcConfig struct {
	Broker   string `json:"broker"`
	ClientID string `json:"clientID"`
	Topic    string `json:"topic"`

	UserName string `json:"userName"`
	Password string `json:"password"`

	QOS       byte `json:"qos"`
	Retained  bool `json:"retained"`
	BatchSize int  `json:"batchSize"`
}

func (c *MQTTSrcConfig) Configure() (kawa.Source[types.Event], error) {
	slog.Info("configuring mqtt")
	return mqttsrc.New(
		mqttsrc.WithBroker(c.Broker),
		mqttsrc.WithClientID(c.ClientID),
		mqttsrc.WithQOS(c.QOS),
		mqttsrc.WithBatchSize(c.BatchSize),
		mqttsrc.WithTopic(c.Topic),
		mqttsrc.WithRetained(c.Retained),
		mqttsrc.WithUserName(c.UserName),
		mqttsrc.WithPassword(c.Password),
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
	return s3.New(
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
