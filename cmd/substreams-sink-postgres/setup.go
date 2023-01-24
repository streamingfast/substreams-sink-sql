package main

import (
	"fmt"

	"github.com/spf13/cobra"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/substreams-sink-postgres/db"
)

var SinkSetupCmd = Command(sinkSetupE,
	"setup <psql_dsn> <schema_file>",
	"Setup the database with the schema and create default cursor table if it does not exist",
	ExactArgs(2),
)

func sinkSetupE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	psqlDSN := args[0]
	schemaFile := args[1]

	dbLoader, err := db.NewLoader(psqlDSN, zlog, tracer)
	if err != nil {
		return fmt.Errorf("new psql loader: %w", err)
	}

	err = dbLoader.Setup(ctx, schemaFile)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	zlog.Info("setup completed successfully")
	return nil
}
