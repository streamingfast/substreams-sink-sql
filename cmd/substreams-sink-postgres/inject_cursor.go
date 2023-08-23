package main

import (
	"encoding/hex"
	"errors"
	"fmt"
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
	"inject-cursor <input-path> <psql-dsn> <manifest> <outputModuleName>",
	"Injects the cursor from a file into database",
	ExactArgs(4),
	Flags(func(flags *pflag.FlagSet) {
		AddCommonSinkerFlags(flags)
	}),
)

func injectCursor(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	inputPath := args[0]
	psqlDSN := args[1]
	manifestPath := args[2]
	outputModuleName := args[3]

	moduleMismatchMode, err := db.ParseOnModuleHashMismatch(sflags.MustGetString(cmd, "on-module-hash-mistmatch"))
	cli.NoError(err, "invalid mistmatch mode")

	t0 := time.Now()

	// update cursor
	zlog.Info("getting sink from manifest")
	_, _, outputModuleHash, err := sink.ReadManifestAndModule(
		manifestPath,
		nil,
		outputModuleName,
		supportedOutputTypes,
		false,
		zlog)
	if err != nil {
		err = fmt.Errorf("read manifest and module: %w", err)
		return err
	}
	moduleHash := hex.EncodeToString(outputModuleHash)
	// get cursor from file

	// probably I need to completly rewrite how cursor are stored.
	zlog.Info("getting cursor from state.yaml")
	stateStorePath := filepath.Join(inputPath, "state.yaml")
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

	zlog.Info("inserting cursor")
	if err := dbLoader.InsertCursor(ctx, moduleHash, fileCursor); err != nil {
		return fmt.Errorf("unable to insert initial cursor: %w", err)
	}
	zlog.Info("cursor written", zap.Duration("total", time.Since(t0)))
	return nil
}
