package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/db"
	"go.uber.org/zap"
)

var supportedOutputTypes = "sf.substreams.sink.database.v1.DatabaseChanges,sf.substreams.database.v1.DatabaseChanges"

func newLoaderAndBaseSinker(
	cmd *cobra.Command,
	psqlDSN string,
	flushInterval time.Duration,
	endpoint, manifestPath, moduleName, blockRange string,
	zlog *zap.Logger,
	tracer logging.Tracer,
	opts ...sink.Option,
) (*db.Loader, *sink.Sinker, error) {
	moduleMismatchMode, err := db.ParseOnModuleHashMismatch(sflags.MustGetString(cmd, "on-module-hash-mistmatch"))
	cli.NoError(err, "invalid mistmatch mode")

	dbLoader, err := db.NewLoader(psqlDSN, flushInterval, moduleMismatchMode, zlog, tracer)
	if err != nil {
		return nil, nil, fmt.Errorf("new psql loader: %w", err)
	}

	if err := dbLoader.LoadTables(); err != nil {
		var e *db.CursorError
		if errors.As(err, &e) {
			fmt.Printf("Error validating the cursors table: %s\n", e)
			fmt.Println("You can use the following sql schema to create a cursors table")
			fmt.Println()
			fmt.Println(dbLoader.GetCreateCursorsTableSQL())
			fmt.Println()
			return nil, nil, fmt.Errorf("invalid cursors table")
		}
		return nil, nil, fmt.Errorf("load psql table: %w", err)
	}

	outputModuleName := sink.InferOutputModuleFromPackage
	if moduleName != "" {
		outputModuleName = moduleName
	}

	sink, err := sink.NewFromViper(
		cmd,
		supportedOutputTypes,
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
		opts...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to setup sinker: %w", err)
	}

	return dbLoader, sink, nil
}

// AddCommonSinkerFlags adds the flags common to all command that needs to create a sinker,
// namely the `run` and `generate-csv` commands.
func AddCommonSinkerFlags(flags *pflag.FlagSet) {
	flags.String("on-module-hash-mistmatch", "error", cli.FlagDescription(`
		What to do when the module hash in the manifest does not match the one in the database, can be 'error', 'warn' or 'ignore'

		- If 'error' is used (default), it will exit with an error explaining the problem and how to fix it.
		- If 'warn' is used, it does the same as 'ignore' but it will log a warning message when it happens.
		- If 'ignore' is set, we pick the cursor at the highest block number and use it as the starting point. Subsequent
		updates to the cursor will overwrite the module hash in the database.
	`))
}
