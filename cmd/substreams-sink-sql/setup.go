package main

import (
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	db2 "github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams/manifest"
)

var sinkSetupCmd = Command(sinkSetupE,
	"setup <dsn> <manifest>",
	"Setup the required infrastructure to deploy a Substreams SQL deployable unit",
	ExactArgs(2),
	Flags(func(flags *pflag.FlagSet) {
		AddCommonDatabaseChangesFlags(flags)

		flags.Bool("postgraphile", false, "Will append the necessary 'comments' on cursors table to fully support postgraphile")
		flags.Bool("system-tables-only", false, "will only create/update the systems tables (cursors, substreams_history) and ignore the schema from the manifest")
		flags.Bool("ignore-duplicate-table-errors", false, "[Dev] Use this if you want to ignore duplicate table errors, take caution that this means the 'schemal.sql' file will not have run fully!")
	}),
)

func sinkSetupE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	dsnString := args[0]
	manifestPath := args[1]
	ignoreDuplicateTableErrors := sflags.MustGetBool(cmd, "ignore-duplicate-table-errors")
	systemTableOnly := sflags.MustGetBool(cmd, "system-tables-only")
	cursorTableName := sflags.MustGetString(cmd, "cursors-table")
	historyTableName := sflags.MustGetString(cmd, "history-table")

	reader, err := manifest.NewReader(manifestPath)
	if err != nil {
		return fmt.Errorf("setup manifest reader: %w", err)
	}
	pkgBundle, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	sinkConfig, err := extractSinkService(pkgBundle.Package)
	if err != nil {
		return fmt.Errorf("extract sink config: %w", err)
	}

	dsn, err := db2.ParseDSN(dsnString)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}

	handleReorgs := false
	dbLoader, err := db2.NewLoader(
		dsn,
		cursorTableName,
		historyTableName,
		sflags.MustGetString(cmd, "clickhouse-cluster"),
		0, 0, 0,
		sflags.MustGetString(cmd, onModuleHashMistmatchFlag),
		&handleReorgs,
		zlog, tracer,
	)

	if err != nil {
		return fmt.Errorf("creating loader: %w", err)
	}

	if err := dbLoader.LoadTables(dsn.Schema(), cursorTableName, historyTableName); err != nil {
		var e *db2.SystemTableError
		if errors.As(err, &e) {
			fmt.Printf("Error validating the system table: %s\n", e)
			fmt.Println("Did you run setup ?")
			return e
		}

		return fmt.Errorf("load psql table: %w", err)
	}

	userSQLSchema := sinkConfig.Schema
	if systemTableOnly {
		userSQLSchema = ""
	}

	err = dbLoader.Setup(ctx, dsn.Schema(), userSQLSchema, sflags.MustGetBool(cmd, "postgraphile"))
	if err != nil {
		if isDuplicateTableError(err) && ignoreDuplicateTableErrors {
			zlog.Info("received duplicate table error, script did not execute successfully")
		} else {
			return fmt.Errorf("setup: %w", err)
		}
	}
	zlog.Info("setup completed successfully")
	return nil
}

func isDuplicateTableError(err error) bool {
	var sqlError *pq.Error
	if !errors.As(err, &sqlError) {
		return false
	}

	// List at https://www.postgresql.org/docs/14/errcodes-appendix.html#ERRCODES-TABLE
	switch sqlError.Code {
	// Error code named `duplicate_table`
	case "42P07":
		return true
	}

	return false
}
