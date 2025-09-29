package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	sink "github.com/streamingfast/substreams-sink"
	sinker2 "github.com/streamingfast/substreams-sink-sql/db_changes/sinker"
	"github.com/streamingfast/substreams/manifest"
)

type ignoreUndoBufferSize struct{}

func (i ignoreUndoBufferSize) IsIgnored(in string) bool {
	return in == "undo-buffer-size"
}

var sinkRunCmd = Command(sinkRunE,
	"run <dsn> <manifest> [<start>:<stop>]",
	"Runs SQL sink process",
	RangeArgs(2, 3),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags, ignoreUndoBufferSize{})
		AddCommonSinkerFlags(flags)
		AddCommonDatabaseChangesFlags(flags)

		flags.Int("undo-buffer-size", 0, "If non-zero, handling of reorgs in the database is disabled. Instead, a buffer is introduced to only process blocks once they have been confirmed by that many blocks, introducing a latency but slightly reducing the load on the database when close to head. Set to 0 to enable reorg handling in the database (required for some databases like Postgres).")
		flags.Int("batch-block-flush-interval", 1_000, "When in catch up mode, flush every N blocks or after batch-row-flush-interval, whichever comes first. Set to 0 to disable and only use batch-row-flush-interval. Ineffective if the sink is now in the live portion of the chain where only 'live-block-flush-interval' applies.")
		flags.Int("batch-row-flush-interval", 100_000, "When in catch up mode, flush every N rows or after batch-block-flush-interval, whichever comes first. Set to 0 to disable and only use batch-block-flush-interval. Ineffective if the sink is now in the live portion of the chain where only 'live-block-flush-interval' applies.")
		flags.Int("live-block-flush-interval", 1, "When processing in live mode, flush every N blocks.")
		flags.Int("flush-interval", 0, "(deprecated) please use --batch-block-flush-interval instead")
		flags.Int("flush-retry-count", 3, "Number of retry attempts for flush operations")
		flags.Duration("flush-retry-delay", 1*time.Second, "Base delay for incremental retry backoff on flush failures")
		flags.StringP("endpoint", "e", "", "Specify the substreams endpoint, ex: `mainnet.eth.streamingfast.io:443`")

		// Postgres insert-only batching (runtime-only flags)
		flags.String("pg-insert-batch-mode", "off", "Postgres insert batching mode: off|values|unnest (runtime-only)")
		flags.Int("pg-insert-batch-size", 1000, "Postgres insert batch size when batching is enabled (runtime-only)")
		flags.Bool("pg-insert-only", false, "Assert insert-only processing; if other ops are present, fallback or error based on future wiring (runtime-only)")
	}),
	Example("substreams-sink-sql run 'postgres://localhost:5432/posgres?sslmode=disable' uniswap-v3@v0.2.10"),
	OnCommandErrorLogAndExit(zlog),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := NewApplication(cmd.Context())

	sink.RegisterMetrics()
	sinker2.RegisterMetrics()

	dsnString := args[0]
	manifestPath := args[1]
	blockRange := ""
	if len(args) > 2 {
		blockRange = args[2]
	}

	endpoint := sflags.MustGetString(cmd, "endpoint")
	if endpoint == "" {
		network := sflags.MustGetString(cmd, "network")
		if network == "" {
			reader, err := manifest.NewReader(manifestPath)
			if err != nil {
				return fmt.Errorf("setup manifest reader: %w", err)
			}
			pkgBundle, err := reader.Read()
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}
			network = pkgBundle.Package.Network
		}
		var err error
		endpoint, err = manifest.ExtractNetworkEndpoint(network, sflags.MustGetString(cmd, "endpoint"), zlog)
		if err != nil {
			return err
		}
	}

	sink, err := sink.NewFromViper(
		cmd,
		supportedOutputTypes,
		endpoint,
		manifestPath,
		sink.InferOutputModuleFromPackage,
		blockRange,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("new base sinker: %w", err)
	}

	batchBlockFlushInterval := sflags.MustGetInt(cmd, "batch-block-flush-interval")
	if sflags.MustGetInt(cmd, "flush-interval") != 0 {
		batchBlockFlushInterval = sflags.MustGetInt(cmd, "flush-interval")
	}
	batchRowFlushInterval := sflags.MustGetInt(cmd, "batch-row-flush-interval")
	liveBlockFlushInterval := sflags.MustGetInt(cmd, "live-block-flush-interval")
	flushRetryCount := sflags.MustGetInt(cmd, "flush-retry-count")
	flushRetryDelay := sflags.MustGetDuration(cmd, "flush-retry-delay")

	// Read Postgres insert batching flags
	pgInsertBatchMode := sflags.MustGetString(cmd, "pg-insert-batch-mode")
	pgInsertBatchSize := sflags.MustGetInt(cmd, "pg-insert-batch-size")
	pgInsertOnly := sflags.MustGetBool(cmd, "pg-insert-only")

	cursorTableName := sflags.MustGetString(cmd, "cursors-table")
	historyTableName := sflags.MustGetString(cmd, "history-table")
	handleReorgs := sflags.MustGetInt(cmd, "undo-buffer-size") == 0

	sinkerFactory := sinker2.SinkerFactory(sink, sinker2.SinkerFactoryOptions{
		CursorTableName:         cursorTableName,
		HistoryTableName:        historyTableName,
		ClickhouseCluster:       sflags.MustGetString(cmd, "clickhouse-cluster"),
		BatchBlockFlushInterval: batchBlockFlushInterval,
		BatchRowFlushInterval:   batchRowFlushInterval,
		LiveBlockFlushInterval:  liveBlockFlushInterval,
		OnModuleHashMismatch:    sflags.MustGetString(cmd, onModuleHashMistmatchFlag),
		HandleReorgs:            handleReorgs,
		FlushRetryCount:         flushRetryCount,
		FlushRetryDelay:         flushRetryDelay,
		PgInsertBatchMode:       pgInsertBatchMode,
		PgInsertBatchSize:       pgInsertBatchSize,
		PgInsertOnly:            pgInsertOnly,
	})

	postgresSinker, err := sinkerFactory(app.Context(), dsnString, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup postgres sinker: %w", err)
	}

	app.SuperviseAndStart(postgresSinker)

	return app.WaitForTermination(zlog, 0*time.Second, 30*time.Second)
}
