package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
	"go.uber.org/zap"
)

var sinkRunCmd = Command(sinkRunE,
	"run <psql_dsn> <endpoint> <manifest> [<module>] [<start>:<stop>]",
	"Runs Postgres sink process",
	RangeArgs(3, 5),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
		AddCommonSinkerFlags(flags)

		flags.Int("flush-interval", 1000, "When in catch up mode, flush every N blocks")
	}),
	OnCommandErrorLogAndExit(zlog),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	psqlDSN := args[0]
	endpoint := args[1]
	manifestPath := args[2]
	moduleName := ""
	if len(args) > 3 {
		moduleName = args[3]
	}
	blockRange := ""
	if len(args) > 4 {
		blockRange = args[4]
	}

	flushInterval := sflags.MustGetDuration(cmd, "flush-interval")

	dbLoader, sink, err := newLoaderAndBaseSinker(cmd, psqlDSN, flushInterval, endpoint, manifestPath, moduleName, blockRange, zlog, tracer)
	if err != nil {
		return fmt.Errorf("instantiate db loader and sink: %w", err)
	}

	postgresSinker, err := sinker.New(sink, dbLoader, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup postgres sinker: %w", err)
	}

	postgresSinker.OnTerminating(app.Shutdown)
	app.OnTerminating(func(err error) {
		postgresSinker.Shutdown(err)
	})

	go func() {
		postgresSinker.Run(ctx)
	}()

	zlog.Info("ready, waiting for signal to quit")

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(0*time.Second, zlog)
	select {
	case <-signalHandler:
		go app.Shutdown(nil)
		break
	case <-app.Terminating():
		zlog.Info("run terminating", zap.Bool("from_signal", isSignaled.Load()), zap.Bool("with_error", app.Err() != nil))
		break
	}

	zlog.Info("waiting for run termination")
	select {
	case <-app.Terminated():
	case <-time.After(30 * time.Second):
		zlog.Warn("application did not terminate within 30s")
	}

	if err := app.Err(); err != nil {
		return err
	}

	zlog.Info("run terminated gracefully")
	return nil
}
