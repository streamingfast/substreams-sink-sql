package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/pflag"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	pbsql "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/services/v1"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

var (
	onModuleHashMistmatchFlag = "on-module-hash-mistmatch"
)

var supportedOutputTypes = "sf.substreams.sink.database.v1.DatabaseChanges,sf.substreams.database.v1.DatabaseChanges"

var (
	supportedDeployableUnits              []string
	deprecated_supportedDeployableService = "sf.substreams.sink.sql.v1.Service"
	supportedDeployableService            = "sf.substreams.sink.sql.service.v1.Service"
)

func init() {
	supportedDeployableUnits = []string{
		deprecated_supportedDeployableService,
	}
}

func extractSinkService(pkg *pbsubstreams.Package) (*pbsql.Service, error) {
	if pkg.SinkConfig == nil {
		return nil, fmt.Errorf("no sink config found in spkg")
	}

	switch pkg.SinkConfig.TypeUrl {
	case deprecated_supportedDeployableService, supportedDeployableService:
		service := &pbsql.Service{}

		if err := proto.Unmarshal(pkg.SinkConfig.Value, service); err != nil {
			return nil, fmt.Errorf("failed to proto unmarshal: %w", err)
		}
		return service, nil
	}

	return nil, fmt.Errorf("invalid config type %q, supported configs are %q", pkg.SinkConfig.TypeUrl, strings.Join(supportedDeployableUnits, ", "))
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

func AddCommonDatabaseChangesFlags(flags *pflag.FlagSet) {
	flags.String("cursors-table", "cursors", "[Operator] Name of the table to use for storing cursors")
	flags.String("history-table", "substreams_history", "[Operator] Name of the table to use for storing block history, used to handle reorgs")
	flags.String("clickhouse-cluster", "", "[Operator] If non-empty, a 'ON CLUSTER <cluster>' clause will be applied when setting up tables in Clickhouse. It will also replace the table engine with it's replicated counterpart (MergeTree will be replaced with ReplicatedMergeTree for example).")
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
