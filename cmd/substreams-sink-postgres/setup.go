package main

import (
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/substreams-sink-postgres/db"
	"go.uber.org/zap"
)

var sinkSetupCmd = Command(sinkSetupE,
	"setup <psql_dsn> <schema_file>",
	"Setup the database with the schema and create default cursor table if it does not exist",
	ExactArgs(2),
	Flags(func(flags *pflag.FlagSet) {
		flags.Bool("ignore-duplicate-table-errors", false, "[Dev] Use this if you want to ignore duplicate table errors, take caution that this means the 'schemal.sql' file will not have run fully!")
	}),
)

func sinkSetupE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	psqlDSN := args[0]
	schemaFile := args[1]
	ignoreDuplicateTableErrors := sflags.MustGetBool(cmd, "ignore-duplicate-table-errors")

	dbLoader, err := db.NewLoader(psqlDSN, 0, db.OnModuleHashMismatchError, zlog, tracer)
	if err != nil {
		return fmt.Errorf("new psql loader: %w", err)
	}

	err = dbLoader.Setup(ctx, schemaFile)
	if err != nil {
		if isDuplicateTableError(err) && ignoreDuplicateTableErrors {
			zlog.Info("received duplicate table error, script dit not executed succesfully completed", zap.String("schema_file", schemaFile))
		} else {
			return fmt.Errorf("setup: %w", err)
		}
	}

	zlog.Info("setup completed successfully")
	return nil
}

func isDuplicateTableError(err error) bool {
	var postgresqlError *pq.Error
	if !errors.As(err, &postgresqlError) {
		return false
	}

	// List at https://www.postgresql.org/docs/14/errcodes-appendix.html#ERRCODES-TABLE
	switch postgresqlError.Code {
	// Error code named `duplicate_table`
	case "42P07":
		return true
	}

	return false
}
