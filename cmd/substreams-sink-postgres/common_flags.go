package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/db"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

var supportedOutputTypes = "sf.substreams.sink.database.v1.DatabaseChanges,sf.substreams.database.v1.DatabaseChanges"

var (
	onModuleHashMistmatchFlag = "on-module-hash-mistmatch"
)

func newDBLoader(
	cmd *cobra.Command,
	psqlDSN string,
	flushInterval time.Duration,
) (*db.Loader, error) {
	moduleMismatchMode, err := db.ParseOnModuleHashMismatch(sflags.MustGetString(cmd, onModuleHashMistmatchFlag))
	cli.NoError(err, "invalid mistmatch mode")

	dbLoader, err := db.NewLoader(psqlDSN, flushInterval, moduleMismatchMode, zlog, tracer)
	if err != nil {
		return nil, fmt.Errorf("new psql loader: %w", err)
	}

	if err := dbLoader.LoadTables(); err != nil {
		var e *db.CursorError
		if errors.As(err, &e) {
			fmt.Printf("Error validating the cursors table: %s\n", e)
			fmt.Println("You can use the following sql schema to create a cursors table")
			fmt.Println()
			fmt.Println(dbLoader.GetCreateCursorsTableSQL())
			fmt.Println()
			return nil, fmt.Errorf("invalid cursors table")
		}

		return nil, fmt.Errorf("load psql table: %w", err)
	}

	return dbLoader, nil
}

func newDBLoaderAndBaseSinker(
	cmd *cobra.Command,
	psqlDSN string,
	flushInterval time.Duration,
	endpoint, manifestPath, moduleName, blockRange string,
	zlog *zap.Logger,
	tracer logging.Tracer,
	opts ...sink.Option,
) (*db.Loader, *sink.Sinker, error) {
	dbLoader, err := newDBLoader(cmd, psqlDSN, flushInterval)
	if err != nil {
		return nil, nil, fmt.Errorf("new db loader: %w", err)
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
		return nil, nil, fmt.Errorf("new base sinker: %w", err)
	}

	return dbLoader, sink, nil
}

// AddCommonSinkerFlags adds the flags common to all command that needs to create a sinker,
// namely the `run` and `generate-csv` commands.
func AddCommonSinkerFlags(flags *pflag.FlagSet) {
	flags.String(onModuleHashMistmatchFlag, "error", cli.FlagDescription(`
		What to do when the module hash in the manifest does not match the one in the database, can be 'error', 'warn' or 'ignore'

		- If 'error' is used (default), it will exit with an error explaining the problem and how to fix it.
		- If 'warn' is used, it does the same as 'ignore' but it will log a warning message when it happens.
		- If 'ignore' is set, we pick the cursor at the highest block number and use it as the starting point. Subsequent
		updates to the cursor will overwrite the module hash in the database.
	`))
}

func readBlockRangeArgument(in string) (blockRange *bstream.Range, err error) {
	return sink.ReadBlockRange(&pbsubstreams.Module{
		Name:         "dummy",
		InitialBlock: 0,
	}, in)
}

type cliApplication struct {
	appCtx  context.Context
	shutter *shutter.Shutter
}

func NewApplication(ctx context.Context) *cliApplication {
	shutter := shutter.New()

	appCtx, cancelApp := context.WithCancel(ctx)
	shutter.OnTerminating(func(_ error) {
		cancelApp()
	})

	return &cliApplication{
		appCtx:  appCtx,
		shutter: shutter,
	}
}

func (a *cliApplication) Context() context.Context {
	return a.appCtx
}

type Shutter interface {
	OnTerminated(f func(error))
	OnTerminating(f func(error))
	Shutdown(error)
}

type Runnable interface {
	Run()
}

type RunnableError interface {
	Run() error
}

type RunnableContext interface {
	Run(ctx context.Context)
}

type RunnableContextError interface {
	Run(ctx context.Context) error
}

// Shutdown is a supervises the received child shutter, mainly, this
// ensures that on child's termination, the application is also terminated
// with the error that caused the child to terminate.
//
// If the application shuts down before the child, the child is also terminated but
// gracefully (it does **not** receive the error that caused the application to terminate).
//
// The child termination is always performed before the application fully complete, unless
// the gracecul shutdown delay has expired.
func (a *cliApplication) Supervise(child Shutter) {
	child.OnTerminated(a.shutter.Shutdown)
	a.shutter.OnTerminating(func(_ error) {
		child.Shutdown(nil)
	})
}

func (a *cliApplication) SuperviseAndStart(child Shutter) {
	child.OnTerminated(a.shutter.Shutdown)
	a.shutter.OnTerminating(func(_ error) {
		child.Shutdown(nil)
	})

	switch v := child.(type) {
	case Runnable:
		go v.Run()
	case RunnableContext:
		go v.Run(a.appCtx)
	case RunnableError:
		go func() {
			err := v.Run()
			if err != nil {
				child.Shutdown(err)
			}
		}()
	case RunnableContextError:
		go func() {
			err := v.Run(a.appCtx)
			if err != nil {
				child.Shutdown(err)
			}
		}()
	}
}

func (a *cliApplication) WaitForTermination(logger *zap.Logger, unreadyPeriodAfterSignal, gracefulShutdownDelay time.Duration) error {
	// On any exit path, we synchronize the logger one last time
	defer func() {
		logger.Sync()
	}()

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(unreadyPeriodAfterSignal, logger)
	select {
	case <-signalHandler:
		go a.shutter.Shutdown(nil)
		break
	case <-a.shutter.Terminating():
		logger.Info("run terminating", zap.Bool("from_signal", isSignaled.Load()), zap.Bool("with_error", a.shutter.Err() != nil))
		break
	}

	logger.Info("waiting for run termination")
	select {
	case <-a.shutter.Terminated():
	case <-time.After(gracefulShutdownDelay):
		logger.Warn("application did not terminate within graceful period of " + gracefulShutdownDelay.String() + ", forcing termination")
	}

	if err := a.shutter.Err(); err != nil {
		return err
	}

	logger.Info("run terminated gracefully")
	return nil
}
