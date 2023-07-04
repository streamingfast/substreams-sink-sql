package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/dstore"
	sink "github.com/streamingfast/substreams-sink"

	"github.com/streamingfast/substreams-sink-postgres/db"
	"github.com/streamingfast/substreams-sink-postgres/state"
	"go.uber.org/zap"
)

var injectCursorCmd = Command(injectCursor,
	"inject-cursor <input-path> <psql-dsn> <endpoint> <manifest> <outputModuleName>",
	"Injects generated CSV rows for <table> into the database pointed by <psql-dsn> argument. Can be run in parallel for multiple rows up to the same stop-block. Watch out, the start-block must be aligned with the range size of the csv files or the module inital block",
	ExactArgs(5),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)

		flags.String("on-module-hash-mistmatch", "error", FlagDescription(`
			What to do when the module hash in the manifest does not match the one in the database, can be 'error', 'warn' or 'ignore'

			- If 'error' is used (default), it will exit with an error explaining the problem and how to fix it.
			- If 'warn' is used, it does the same as 'ignore' but it will log a warning message when it happens.
			- If 'ignore' is set, we pick the cursor at the highest block number and use it as the starting point. Subsequent
			updates to the cursor will overwrite the module hash in the database.
		`),
		)
	}),
)

func injectCursor(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	inputPath := args[0]
	psqlDSN := args[1]
	// we don't need the block information, it's just used to get the sinker module hash
	blockRange := "0:1"
	endpoint := args[2]
	manifestPath := args[3]
	outputModuleName := args[4]

	moduleMismatchMode, err := db.ParseOnModuleHashMismatch(sflags.MustGetString(cmd, "on-module-hash-mistmatch"))
	cli.NoError(err, "invalid mistmatch mode")

	t0 := time.Now()

	// update cursor
	zlog.Info("getting sink from manifest")
	// if someone knows a better way to get the module hash, feel free to update it
	// look for module hash
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
	moduleHash := sink.OutputModuleHash()
	// get cursor from file

	// probably I need to completly rewrite how cursor are stored.
	zlog.Info("getting cursor from state.yaml")
	stateStorePath := filepath.Join(inputPath, "state.yaml")
	stateFileDirectory := filepath.Dir(stateStorePath)
	if err := os.MkdirAll(stateFileDirectory, os.ModePerm); err != nil {
		return fmt.Errorf("create state file directories: %w", err)
	}
	stateDStore, err := dstore.NewStore(inputPath, "", "", false)
	if err != nil {
		return err
	}
	stateStore, err := state.NewFileStateStore(stateStorePath, stateDStore, zlog)
	if err != nil {
		return fmt.Errorf("new file state store: %w", err)
	}

	fileCursor, err := stateStore.ReadCursor(ctx)
	if err != nil && !errors.Is(err, db.ErrCursorNotFound) {
		return fmt.Errorf("unable to retrieve cursor: %w", err)
	}

	zlog.Info("getting cursor inside postgres")
	// we don't need flush interval because it's a single insert/update
	var flushInterval time.Duration = 0

	// we need dbloader to get/update the cursor
	dbLoader, err := db.NewLoader(psqlDSN, flushInterval, moduleMismatchMode, zlog, tracer)
	if err != nil {
		return fmt.Errorf("new psql loader: %w", err)
	}

	// for some reason, we need to load the tables so the insert doesn't crash
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

	// load the cursor from the database
	_, mistmatchDetected, err := dbLoader.GetCursor(ctx, moduleHash)
	if err != nil && !errors.Is(err, db.ErrCursorNotFound) {
		return fmt.Errorf("unable to retrieve cursor: %w", err)
	}

	zlog.Info("updating cursor")
	// cursor not found
	if errors.Is(err, db.ErrCursorNotFound) {
		if err := dbLoader.InsertCursor(ctx, moduleHash, fileCursor); err != nil {
			return fmt.Errorf("unable to insert initial cursor: %w", err)
		}
		// there's a cursor, but it isn't the same hash
	} else if mistmatchDetected {
		if err := dbLoader.InsertCursor(ctx, moduleHash, fileCursor); err != nil {
			return fmt.Errorf("unable to insert initial cursor: %w", err)
		}
		// cursor found and it's the same hash, update it
	} else {
		if err := dbLoader.UpdateCursor(ctx, nil, moduleHash, fileCursor); err != nil {
			return fmt.Errorf("update cursor: %w", err)
		}
	}
	zlog.Info("cursor written", zap.Duration("total", time.Since(t0)))
	return nil
}
