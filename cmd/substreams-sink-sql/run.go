package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/sinker"
	"github.com/streamingfast/substreams/manifest"
)

var sinkRunCmd = Command(sinkRunE,
	"run <dsn> <manifest> [<start>:<stop>]",
	"Runs SQL sink process",
	RangeArgs(2, 3),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
		AddCommonSinkerFlags(flags)

		flags.Int("flush-interval", 1000, "When in catch up mode, flush every N blocks")
		flags.StringP("endpoint", "e", "", "Specify the substreams endpoint, ex: `mainnet.eth.streamingfast.io:443`")
	}),
	OnCommandErrorLogAndExit(zlog),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := NewApplication(cmd.Context())

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	dsn := args[0]
	manifestPath := args[1]
	blockRange := ""
	if len(args) > 2 {
		blockRange = args[2]
	}

	reader, err := manifest.NewReader(manifestPath)
	if err != nil {
		return fmt.Errorf("setup manifest reader: %w", err)
	}
	pkg, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	endpoint, err := manifest.ExtractNetworkEndpoint(pkg.Network, sflags.MustGetString(cmd, "endpoint"), zlog)
	if err != nil {
		return err
	}

	// "github.com/streamingfast/substreams/manifest"
	sink, err := sink.NewFromViper(
		cmd,
		supportedOutputTypes,
		endpoint,
		manifestPath,
		sink.InferOutputModuleFromPackage,
		blockRange,
		zlog,
		tracer,
		sink.WithFinalBlocksOnly(),
	)
	if err != nil {
		return fmt.Errorf("new base sinker: %w", err)
	}

	dbLoader, err := newDBLoader(cmd, dsn, sflags.MustGetDuration(cmd, "flush-interval"))
	if err != nil {
		return fmt.Errorf("new db loader: %w", err)
	}

	postgresSinker, err := sinker.New(sink, dbLoader, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup postgres sinker: %w", err)
	}

	app.SuperviseAndStart(postgresSinker)

	return app.WaitForTermination(zlog, 0*time.Second, 30*time.Second)
}
