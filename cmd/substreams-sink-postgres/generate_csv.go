package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
	"go.uber.org/zap"
)

var generateCsvCmd = Command(generateCsvE,
	"generate-csv <psql_dsn> <endpoint> <dest-folder> <manifest> <module> [start]:<stop>",
	"Generates CSVs for each table so it can then be bulk inserted with `inject-csv`",
	Description(`
		This command command is the first of a multi-step process to bulk insert data into a Postgres database.
		It creates a folder for each table and generates CSVs for block ranges. This files can be used with
		the 'inject-csv' command to bulk insert data into the database.

		It needs that the database already exists and that the schema is already created.

		The process is as follows:

		- Generate CSVs for each table with this command
		- Inject the CSVs into the database with the 'inject-csv' command
		- Run the 'inject-cursor' command to update the cursor in the database
		- Start streaming with the 'run' command
	`),
	ExactArgs(6),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags, sink.FlagIgnore("final-blocks-only"))
		AddCommonSinkerFlags(flags)

		flags.Uint64("bundle-size", 1000, "Size of output bundle, in blocks")
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
	app := shutter.New()

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	psqlDSN := args[0]
	endpoint := args[1]
	destFolder := args[2]
	manifestPath := args[3]
	moduleName := args[4]
	blockRange := args[5]

	bundleSize := sflags.MustGetUint64(cmd, "bundle-size")
	bufferMaxSize := sflags.MustGetUint64(cmd, "buffer-max-size")
	workingDir := sflags.MustGetString(cmd, "working-dir")

	dbLoader, sink, err := newLoaderAndBaseSinker(
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

	generateCSVSinker, err := sinker.NewGenerateCSVSinker(sink, destFolder, workingDir, bundleSize, bufferMaxSize, dbLoader, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup generate csv sinker: %w", err)
	}

	generateCSVSinker.OnTerminated(app.Shutdown)
	app.OnTerminating(func(_ error) {
		generateCSVSinker.Shutdown(nil)
	})

	go func() {
		generateCSVSinker.Run(ctx)
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
