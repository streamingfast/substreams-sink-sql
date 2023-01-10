package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams-sink-postgres/db"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
)

var SinkRunCmd = Command(sinkRunE,
	"run <psql_dsn> <endpoint> <manifest> <module> [<start>:<stop>]",
	"Runs  extractor code",
	RangeArgs(4, 5),
	Flags(func(flags *pflag.FlagSet) {
		flags.BoolP("insecure", "k", false, "Skip certificate validation on GRPC connection")
		flags.BoolP("plaintext", "p", false, "Establish GRPC connection in plaintext")
		flags.Int("undo-buffer-size", 0, "Number of blocks to keep buffered to handle fork reorganizations")
	}),
	AfterAllHook(func(_ *cobra.Command) {
		sinker.RegisterMetrics()
	}),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	psqlDSN := args[0]
	endpoint := args[1]
	manifestPath := args[2]
	outputModuleName := args[3]
	blockRange := ""
	if len(args) > 4 {
		blockRange = args[4]
	}

	zlog.Info("sink from psql",
		zap.String("dsn", psqlDSN),
		zap.String("endpoint", endpoint),
		zap.String("manifest_path", manifestPath),
		zap.String("output_module_name", outputModuleName),
		zap.String("block_range", blockRange),
	)

	dbLoader, err := db.NewLoader(psqlDSN, zlog, tracer)
	if err != nil {
		return fmt.Errorf("new psql loader: %w", err)
	}

	if err := dbLoader.LoadTables(); err != nil {
		var e *db.CursorError
		if errors.As(err, &e) {
			fmt.Println("Error validating the cursors table: ", e.Error())
			fmt.Println("You can use the following sql schema to create a cursors table")
			fmt.Println(`
create table cursors
(
	id         bigserial not null constraint cursor_pk primary key,
	cursors    text,
	block_num  bigint,
	block_id   text
);
			`)
			return fmt.Errorf("invalid cursors table")
		}
		return fmt.Errorf("load psql table: %w", err)
	}

	zlog.Info("reading substreams manifest", zap.String("manifest_path", manifestPath))
	pkg, err := manifest.NewReader(manifestPath).Read()
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	graph, err := manifest.NewModuleGraph(pkg.Modules.Modules)
	if err != nil {
		return fmt.Errorf("create substreams moduel graph: %w", err)
	}

	zlog.Info("validating output store", zap.String("output_store", outputModuleName))
	module, err := graph.Module(outputModuleName)
	if err != nil {
		return fmt.Errorf("get output module %q: %w", outputModuleName, err)
	}
	if module.GetKindMap() == nil {
		return fmt.Errorf("ouput module %q is *not* of  type 'Mapper'", outputModuleName)
	}

	if module.Output.Type != "proto:sf.substreams.database.v1.DatabaseChanges" {
		return fmt.Errorf("postgresql sync only supports maps with output type 'proto:sf.substreams.database.v1.DatabaseChanges'")
	}
	hashes := manifest.NewModuleHashes()
	outputModuleHash := hashes.HashModule(pkg.Modules, module, graph)

	resolvedStartBlock, resolvedStopBlock, err := readBlockRange(module, blockRange)
	if err != nil {
		return fmt.Errorf("resolve block range: %w", err)
	}
	zlog.Info("resolved block range",
		zap.Int64("start_block", resolvedStartBlock),
		zap.Uint64("stop_block", resolvedStopBlock),
	)

	apiToken := readAPIToken()
	config := &sinker.Config{
		DBLoader:         dbLoader,
		BlockRange:       blockRange,
		Pkg:              pkg,
		OutputModule:     module,
		OutputModuleName: outputModuleName,
		OutputModuleHash: outputModuleHash,
		UndoBufferSize:   viper.GetInt("run-undo-buffer-size"),
		ClientConfig: client.NewSubstreamsClientConfig(
			endpoint,
			apiToken,
			viper.GetBool("run-insecure"),
			viper.GetBool("run-plaintext"),
		),
	}

	postgresSinker, err := sinker.New(
		config,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}
	postgresSinker.OnTerminating(app.Shutdown)

	app.OnTerminating(func(err error) {
		zlog.Info("application terminating shutting down sinker")
		postgresSinker.Shutdown(err)
	})

	go func() {
		if err := postgresSinker.Start(ctx); err != nil {
			zlog.Error("sinker failed", zap.Error(err))
			postgresSinker.Shutdown(err)
		}
	}()

	signalHandler := derr.SetupSignalHandler(0 * time.Second)
	zlog.Info("ready, waiting for signal to quit")
	select {
	case <-signalHandler:
		zlog.Info("received termination signal, quitting application")
		go app.Shutdown(nil)
	case <-app.Terminating():
		NoError(app.Err(), "application shutdown unexpectedly, quitting")
	}

	zlog.Info("waiting for app termination")
	select {
	case <-app.Terminated():
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
		zlog.Error("application did not terminated within 30s, forcing exit")
	}

	zlog.Info("app terminated")
	return nil
}
