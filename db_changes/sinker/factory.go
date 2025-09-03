package sinker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"go.uber.org/zap"
)

var _ = time.Second // keep import

type SinkerFactoryFunc func(ctx context.Context, dsnString string, logger *zap.Logger, tracer logging.Tracer) (*SQLSinker, error)

type SinkerFactoryOptions struct {
	CursorTableName         string
	HistoryTableName        string
	ClickhouseCluster       string
	BatchBlockFlushInterval int
	BatchRowFlushInterval   int
	LiveBlockFlushInterval  int
	OnModuleHashMismatch    string
	HandleReorgs            bool
	FlushRetryCount         int
	FlushRetryDelay         time.Duration
	// Postgres insert batching (runtime flags wired in from run.go)
	PgInsertBatchMode string // off|values|unnest
	PgInsertBatchSize int
	PgInsertOnly      bool
}

func SinkerFactory(
	baseSink *sink.Sinker,
	options SinkerFactoryOptions,
) SinkerFactoryFunc {
	return func(ctx context.Context, dsnString string, logger *zap.Logger, tracer logging.Tracer) (*SQLSinker, error) {
		dsn, err := db.ParseDSN(dsnString)
		if err != nil {
			return nil, fmt.Errorf("parsing dsn: %w", err)
		}

		dbLoader, err := db.NewLoader(
			dsn,
			options.CursorTableName,
			options.HistoryTableName,
			options.ClickhouseCluster,
			options.BatchBlockFlushInterval,
			options.BatchRowFlushInterval,
			options.LiveBlockFlushInterval,
			options.OnModuleHashMismatch,
			&options.HandleReorgs,
			logger,
			tracer,
		)
		if err != nil {
			return nil, fmt.Errorf("creating loader: %w", err)
		}

		// Configure Postgres insert batching on the loader (no-op for other dialects)
		dbLoader.ConfigurePgInsertBatching(options.PgInsertBatchMode, options.PgInsertBatchSize, options.PgInsertOnly)
		logger.Info("pg insert batching settings",
			zap.String("mode", options.PgInsertBatchMode),
			zap.Int("batch_size", options.PgInsertBatchSize),
			zap.Bool("insert_only", options.PgInsertOnly),
		)

		if err := dbLoader.LoadTables(dsn.Schema(), options.CursorTableName, options.HistoryTableName); err != nil {
			var e *db.SystemTableError
			if errors.As(err, &e) {
				return nil, fmt.Errorf("error validating the system table: %w. Did you run setup?", e)
			}
			return nil, fmt.Errorf("load psql table: %w", err)
		}

		sinker, err := New(baseSink, dbLoader, logger, tracer, options.FlushRetryCount, options.FlushRetryDelay)
		if err != nil {
			return nil, fmt.Errorf("unable to setup SQL sinker: %w", err)
		}

		return sinker, nil
	}
}
