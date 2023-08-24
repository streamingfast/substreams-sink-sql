package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
)

var sinkRunCmd = Command(sinkRunE,
	"run <psql_dsn> <endpoint> <manifest> <module> [<start>:<stop>]",
	"Runs Postgres sink process",
	RangeArgs(4, 5),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
		AddCommonSinkerFlags(flags)

		flags.Int("flush-interval", 1000, "When in catch up mode, flush every N blocks")
	}),
	OnCommandErrorLogAndExit(zlog),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := NewApplication(cmd.Context())

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	psqlDSN := args[0]
	endpoint := args[1]
	manifestPath := args[2]
	moduleName := args[3]
	blockRange := ""
	if len(args) > 4 {
		blockRange = args[4]
	}

	dbLoader, sink, err := newDBLoaderAndBaseSinker(
		cmd,
		psqlDSN,
		sflags.MustGetDuration(cmd, "flush-interval"),
		endpoint, manifestPath, moduleName, blockRange,
		zlog, tracer,
	)
	if err != nil {
		return fmt.Errorf("instantiate db loader and sink: %w", err)
	}

	postgresSinker, err := sinker.New(sink, dbLoader, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup postgres sinker: %w", err)
	}

	app.SuperviseAndStart(postgresSinker)

	return app.WaitForTermination(zlog, 0*time.Second, 30*time.Second)
}
