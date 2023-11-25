package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"path"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/cmd/kawad/internal/queue"
	"github.com/runreveal/kawa/cmd/kawad/internal/types"
	"github.com/runreveal/lib/await"
	"github.com/runreveal/lib/loader"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slog"
)

var (
	version = "dev"
)

func init() {
	replace := func(groups []string, a slog.Attr) slog.Attr {
		// Remove the directory from the source's filename.
		if a.Key == slog.SourceKey {
			source := a.Value.Any().(*slog.Source)
			source.File = filepath.Base(source.File)
		}
		return a
	}
	level := slog.LevelInfo
	if _, ok := os.LookupEnv("KAWA_DEBUG"); ok {
		level = slog.LevelDebug
	}

	h := slog.NewTextHandler(
		os.Stderr,
		&slog.HandlerOptions{
			Level:       level,
			AddSource:   true,
			ReplaceAttr: replace,
		},
	)

	slogger := slog.New(h)
	slog.SetDefault(slogger)
}

func main() {
	slog.Info(fmt.Sprintf("starting %s", path.Base(os.Args[0])), "version", version)
	rootCmd := NewRootCommand()
	kawaCmd := NewRunCommand()
	rootCmd.AddCommand(kawaCmd)

	if err := rootCmd.Execute(); err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}

// Build the cobra command that handles our command line tool.
func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   path.Base(os.Args[0]),
		Short: `kawa is an all-in-one event ingestion daemon`,
		Long: `kawa is an all-in-one event ingestion daemon.
It is designed to be a single binary that can be deployed to a server and	
configured to receive events from a variety of sources and send them to a 
variety of destinations.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	return rootCmd
}

type MonConfig struct {
	Addr  string `json:"addr"`
	PProf struct {
		Path string `json:"path"`
	} `json:"pprof"`
	Metrics struct {
		Path string `json:"path"`
	} `json:"metrics"`
}

type Config struct {
	Sources      map[string]loader.Loader[kawa.Source[types.Event]]      `json:"sources"`
	Destinations map[string]loader.Loader[kawa.Destination[types.Event]] `json:"destinations"`

	Monitoring MonConfig `json:"monitoring"`
}

// Build the cobra command that handles our command line tool.
func NewRunCommand() *cobra.Command {
	// Use configuration defined outside the main package 🎉
	var config Config
	var configFile string

	cmd := &cobra.Command{
		Use:   "run",
		Short: "run the all-in-one event ingestion daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			bts, err := os.ReadFile(configFile)
			if err != nil {
				return err
			}
			err = loader.LoadConfig(bts, &config)
			if err != nil {
				return err
			}

			w := await.New(await.WithSignals)

			if config.Monitoring.Addr != "" {
				mux := http.NewServeMux()
				if config.Monitoring.PProf.Path != "" {
					prefix := config.Monitoring.PProf.Path
					http.HandleFunc(prefix, pprof.Index)
					http.HandleFunc(prefix+"cmdline", pprof.Cmdline)
					http.HandleFunc(prefix+"profile", pprof.Profile)
					http.HandleFunc(prefix+"symbol", pprof.Symbol)
					http.HandleFunc(prefix+"trace", pprof.Trace)
				}
				if config.Monitoring.Metrics.Path != "" {
					mux.Handle(config.Monitoring.Metrics.Path, promhttp.Handler())
				}
				server := &http.Server{Addr: config.Monitoring.Addr, Handler: mux}
				w.AddNamed(await.ListenAndServe(server), "monitoring")
			}

			slog.Info(fmt.Sprintf("config: %+v", config))

			ctx := context.Background()
			srcs := map[string]queue.Source{}
			for k, v := range config.Sources {
				src, err := v.Configure()
				if err != nil {
					return err
				}
				srcs[k] = queue.Source{Name: k, Source: src}
			}

			dsts := map[string]queue.Destination{}
			for k, v := range config.Destinations {
				dst, err := v.Configure()
				if err != nil {
					return err
				}
				dsts[k] = queue.Destination{Name: k, Destination: dst}
			}

			q := queue.New(queue.WithSources(srcs), queue.WithDestinations(dsts))
			w.AddNamed(q, "queue")
			err = w.Run(ctx)
			slog.Error(fmt.Sprintf("closing: %+v", err))
			return err
		},
	}

	cmd.Flags().StringVar(&configFile, "config", "config.json", "where to load the configuration from")
	err := cmd.MarkFlagRequired("config")
	if err != nil {
		panic(err)
	}

	return cmd
}
