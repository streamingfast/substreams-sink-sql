package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/db"
	"github.com/streamingfast/substreams-sink-postgres/rollback_sinker"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
	"go.uber.org/zap"
)

var sinkRunCmd = Command(sinkRunE,
	"run <psql_dsn> <endpoint> <manifest> <module> [<start>:<stop>]",
	"Runs Postgres sink process",
	RangeArgs(4, 5),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)

		flags.Int("flush-interval", 1000, "When in catch up mode, flush every N blocks")
		flags.String("on-module-hash-mistmatch", "error", FlagDescription(`
			What to do when the module hash in the manifest does not match the one in the database, can be 'error', 'warn' or 'ignore'

			- If 'error' is used (default), it will exit with an error explaining the problem and how to fix it.
			- If 'warn' is used, it does the same as 'ignore' but it will log a warning message when it happens.
			- If 'ignore' is set, we pick the cursor at the highest block number and use it as the starting point. Subsequent
			updates to the cursor will overwrite the module hash in the database.
		`),
		)
		flags.String("rollback-url", "", "url of the rollback service")
		flags.String("rollback-db-schema", "", "schema rollback service")
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
	outputModuleName := args[3]
	blockRange := ""
	if len(args) > 4 {
		blockRange = args[4]
	}

	flushInterval := sflags.MustGetDuration(cmd, "flush-interval")
	moduleMismatchMode, err := db.ParseOnModuleHashMismatch(sflags.MustGetString(cmd, "on-module-hash-mistmatch"))
	cli.NoError(err, "invalid mistmatch mode")

	dbLoader, err := db.NewLoader(psqlDSN, flushInterval, moduleMismatchMode, zlog, tracer)
	if err != nil {
		return fmt.Errorf("new psql loader: %w", err)
	}

	if err := dbLoader.LoadTables(); err != nil {
		var e *db.CursorError
		if errors.As(err, &e) {
			fmt.Printf("Error validating the cursors table: %s\n", e)
			fmt.Println("You can use the following sql schema to create a cursors table")
			fmt.Println()
			fmt.Println(dbLoader.GetCreateCursorsTableSQL())
			fmt.Println()
			return fmt.Errorf("invalid cursors table")
		}
		return fmt.Errorf("load psql table: %w", err)
	}

	sink, err := sink.NewFromViper(
		cmd,
		"sf.substreams.sink.database.v1.DatabaseChanges,sf.substreams.database.v1.DatabaseChanges",
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	postgresSinker, err := sinker.New(sink, dbLoader, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup postgres sinker: %w", err)
	}

	rollbackSinker, err := rollback_sinker.NewFromViper(cmd, postgresSinker, zlog)

	zlog.Info("rollback sinker created")

	if err != nil {
		return fmt.Errorf("unable to setup rollback sinker: %w", err)
	}

	rollbackSinker.OnTerminating(app.Shutdown)

	app.OnTerminating(func(err error) {
		rollbackSinker.Shutdown(err)
	})

	go func() {
		rollbackSinker.Run(ctx)
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
