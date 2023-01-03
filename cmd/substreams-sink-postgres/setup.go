package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/substreams-sink-postgres/db"
)

var SinkSetupCmd = Command(sinkSetupE,
	"setup <psql_dsn> <schema_file>",
	"Setup the database with the schema and create default cursor table if it does not exist",
	RangeArgs(2, 2),
	Flags(func(flags *pflag.FlagSet) {}),
	AfterAllHook(func(_ *cobra.Command) {}),
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

	return nil
}
