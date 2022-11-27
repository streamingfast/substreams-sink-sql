package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

// Commit sha1 value, injected via go build `ldflags` at build time
var commit = ""

// Version value, injected via go build `ldflags` at build time
var version = "dev"

// Date value, injected via go build `ldflags` at build time
var date = ""

var zlog, tracer = logging.RootLogger("sink-postgres", "github.com/streamingfast/substreams-sink-postgres/cmd/substreams-sink-postgres")

func init() {
	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))
}

func main() {
	Run("substreams-sink-postgres", "Substreams Postgres Sink",
		SinkRunCmd,

		ConfigureViper("SINK"),
		ConfigureVersion(),

		PersistentFlags(
			func(flags *pflag.FlagSet) {
				flags.Duration("delay-before-start", 0, "[OPERATOR] Amount of time to wait before starting any internal processes, can be used to perform to maintenance on the pod before actually letting it starts")
				flags.String("metrics-listen-addr", "localhost:9102", "[OPERATOR] If non-empty, the process will listen on this address for Prometheus metrics request(s)")
				flags.String("pprof-listen-addr", "localhost:6060", "[OPERATOR] If non-empty, the process will listen on this address for pprof analysis (see https://golang.org/pkg/net/http/pprof/)")
			},
		),
		AfterAllHook(func(cmd *cobra.Command) {
			cmd.PersistentPreRun = func(_ *cobra.Command, _ []string) {
				delay := viper.GetDuration("global-delay-before-start")
				if delay > 0 {
					zlog.Info("sleeping to respect delay before start setting", zap.Duration("delay", delay))
					time.Sleep(delay)
				}

				if v := viper.GetString("global-metrics-listen-addr"); v != "" {
					zlog.Info("starting prometheus metrics server", zap.String("listen_addr", v))
					go dmetrics.Serve(v)
				}

				if v := viper.GetString("global-pprof-listen-addr"); v != "" {
					go func() {
						zlog.Info("starting pprof server", zap.String("listen_addr", v))
						err := http.ListenAndServe(v, nil)
						if err != nil {
							zlog.Debug("unable to start profiling server", zap.Error(err), zap.String("listen_addr", v))
						}
					}()
				}
			}
		}),
	)
}

func ConfigureVersion() CommandOption {
	return CommandOptionFunc(func(cmd *cobra.Command) {
		var labels []string
		if len(commit) >= 7 {
			labels = append(labels, fmt.Sprintf("Commit %s", commit[0:7]))
		}

		if date != "" {
			labels = append(labels, fmt.Sprintf("Built %s", date))
		}

		cmd.Version = version

		if len(labels) != 0 {
			cmd.Version = fmt.Sprintf("%s (%s)", version, strings.Join(labels, ", "))
		}
	})
}
