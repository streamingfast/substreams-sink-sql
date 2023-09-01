package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
)

// lastCursorFilename is the name of the file where the last cursor is stored, no extension as it's added by the store
const lastCursorFilename = "last_cursor"

var generateCsvCmd = Command(generateCsvE,
	"generate-csv <psql_dsn> <endpoint> <manifest> <module> <dest-folder> [start]:<stop>",
	"Generates CSVs for each table so it can be bulk inserted with `inject-csv`",
	Description(`
		This command command is the first of a multi-step process to bulk insert data into a Postgres database.
		It creates a folder for each table and generates CSVs for block ranges. This files can be used with
		the 'inject-csv' command to bulk insert data into the database.

		It needs that the database already exists and that the schema is already created.

		The process is as follows:

		- Generate CSVs for each table with this command
		- Inject the CSVs into the database with the 'inject-csv' command (contains 'cursors' table, double check you injected it correctly!)
		- Start streaming with the 'run' command
	`),
	ExactArgs(6),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags, sink.FlagIgnore("final-blocks-only"))
		AddCommonSinkerFlags(flags)

		flags.Uint64("bundle-size", 10000, "Size of output bundle, in blocks")
		flags.String("working-dir", "./workdir", "Path to local folder used as working directory")
		flags.Uint64("buffer-max-size", 4*1024*1024, FlagDescription(`
			Amount of memory bytes to allocate to the buffered writer. If your data set is small enough that every is hold in memory, we are going to avoid
			the local I/O operation(s) and upload accumulated content in memory directly to final storage location.

			Ideally, you should set this as to about 80%% of RAM the process has access to. This will maximize amout of element in memory,
			and reduce 'syscall' and I/O operations to write to the temporary file as we are buffering a lot of data.

			This setting has probably the greatest impact on writting throughput.

			Default value for the buffer is 4 MiB.
		`))
	}),
	OnCommandErrorLogAndExit(zlog),
)

func generateCsvE(cmd *cobra.Command, args []string) error {
	app := NewApplication(cmd.Context())

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	psqlDSN := args[0]
	endpoint := args[1]
	manifestPath := args[2]
	moduleName := args[3]
	destFolder := args[4]
	blockRange := args[5]

	bundleSize := sflags.MustGetUint64(cmd, "bundle-size")
	bufferMaxSize := sflags.MustGetUint64(cmd, "buffer-max-size")
	workingDir := sflags.MustGetString(cmd, "working-dir")

	dbLoader, sink, err := newDBLoaderAndBaseSinker(
		cmd,
		psqlDSN,
		time.Duration(bundleSize),
		endpoint, manifestPath, moduleName, blockRange,
		zlog, tracer,
		sink.WithFinalBlocksOnly(),
	)
	if err != nil {
		return fmt.Errorf("instantiate db loader and sink: %w", err)
	}

	generateCSVSinker, err := sinker.NewGenerateCSVSinker(sink, destFolder, workingDir, bundleSize, bufferMaxSize, dbLoader, lastCursorFilename, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup generate csv sinker: %w", err)
	}

	app.Supervise(generateCSVSinker.Shutter)

	go func() {
		generateCSVSinker.Run(app.Context())
	}()

	return app.WaitForTermination(zlog, 0*time.Second, 30*time.Second)
}
