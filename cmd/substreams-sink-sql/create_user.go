package main

import (
	"fmt"
	"os"

	"github.com/streamingfast/cli/sflags"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/substreams-sink-sql/db"
)

var createUserCmd = Command(createUserE,
	"create-user <dsn> <username>",
	"Create a user in the database",
	ExactArgs(2),
	Flags(func(flags *pflag.FlagSet) {
		flags.Bool("read-only", false, "Create a read-only user")
		flags.String("password-env", "", "Name of the environment variable containing the password")
	}),
)

func createUserE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	dsn := args[0]
	username := args[1]

	readOnly := sflags.MustGetBool(cmd, "read-only")
	passwordEnv := sflags.MustGetString(cmd, "password-env")

	if passwordEnv == "" {
		return fmt.Errorf("password-env is required")
	}

	password := os.Getenv(passwordEnv)
	if password == "" {
		return fmt.Errorf("non-empty password is required")
	}

	dbLoader, err := db.NewLoader(dsn, 0, db.OnModuleHashMismatchError, nil, zlog, tracer)
	if err != nil {
		return fmt.Errorf("new psql loader: %w", err)
	}

	err = dbLoader.CreateUser(ctx, username, password, "substreams", readOnly)
	if err != nil {
		return fmt.Errorf("create user: %w", err)
	}

	return nil
}
