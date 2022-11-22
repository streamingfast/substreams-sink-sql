package sinker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/streamingfast/substreams/client"

	"github.com/streamingfast/substreams/manifest"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/db"
	pbddatabase "github.com/streamingfast/substreams-sink-postgres/pb/database/v1"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const BLOCK_PROGRESS = 1000

type Config struct {
	DBLoader         *db.Loader
	BlockRange       string
	Pkg              *pbsubstreams.Package
	OutputModule     *pbsubstreams.Module
	OutputModuleName string
	OutputModuleHash manifest.ModuleHash
	ClientConfig     *client.SubstreamsClientConfig
}

type PostgresSinker struct {
	*shutter.Shutter

	DBLoader         *db.Loader
	Pkg              *pbsubstreams.Package
	OutputModule     *pbsubstreams.Module
	OutputModuleName string
	OutputModuleHash manifest.ModuleHash
	ClientConfig     *client.SubstreamsClientConfig

	sink  *sink.Sinker
	stats *Stats

	blockRange *bstream.Range

	logger *zap.Logger
	tracer logging.Tracer
}

func New(config *Config, logger *zap.Logger, tracer logging.Tracer) (*PostgresSinker, error) {
	s := &PostgresSinker{
		Shutter: shutter.New(),
		stats:   NewStats(logger),
		logger:  logger,
		tracer:  tracer,

		DBLoader:         config.DBLoader,
		Pkg:              config.Pkg,
		OutputModule:     config.OutputModule,
		OutputModuleName: config.OutputModuleName,
		OutputModuleHash: config.OutputModuleHash,
		ClientConfig:     config.ClientConfig,
	}

	var err error
	s.blockRange, err = resolveBlockRange(config.BlockRange, config.OutputModule)
	if err != nil {
		return nil, fmt.Errorf("resolve block range: %w", err)
	}

	return s, nil
}

func (s *PostgresSinker) Start(ctx context.Context) error {
	cursor, err := s.DBLoader.GetCursor(hex.EncodeToString(s.OutputModuleHash))
	if err != nil && !errors.Is(err, db.ErrCursorNotFound) {
		return fmt.Errorf("unable to retrieve cursor: %w", err)
	}

	if errors.Is(err, db.ErrCursorNotFound) {
		cursorStartBlock := s.OutputModule.InitialBlock
		if s.blockRange.StartBlock() > 0 {
			cursorStartBlock = s.blockRange.StartBlock() - 1
		}

		cursor = sink.NewCursor("", bstream.NewBlockRef("", cursorStartBlock))

		if err = s.DBLoader.WriteCursor(hex.EncodeToString(s.OutputModuleHash), cursor); err != nil {
			return fmt.Errorf("failed to create initial cursor: %w", err)
		}
	}

	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })
	s.stats.Start(2 * time.Second)

	return s.Run(ctx)
}

func (s *PostgresSinker) Run(ctx context.Context) error {
	cursor, err := s.DBLoader.GetCursor(hex.EncodeToString(s.OutputModuleHash))
	if err != nil {
		return fmt.Errorf("unable to retrieve cursor: %w", err)
	}

	s.sink, err = sink.New(
		s.Pkg.Modules,
		s.OutputModule,
		s.OutputModuleHash,
		s.handleBlockScopeData,
		s.ClientConfig,
		s.logger,
		s.tracer,
	)
	if err != nil {
		return fmt.Errorf("unable to create sink: %w", err)
	}

	s.sink.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.logger.Info("terminating sink")
		s.sink.Shutdown(err)
	})

	if err := s.sink.Start(ctx, s.blockRange, cursor); err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}

	return nil
}

func (s *PostgresSinker) applyDatabaseChanges(dbChanges *pbddatabase.DatabaseChanges) error {
	for _, change := range dbChanges.TableChanges {
		if !s.DBLoader.HasTable(change.Table) {
			continue
		}

		primaryKey := change.Pk
		changes := map[string]string{}
		for _, field := range change.Fields {
			changes[field.Name] = field.NewValue
		}

		switch change.Operation {
		case pbddatabase.TableChange_CREATE:
			err := s.DBLoader.Insert(change.Table, primaryKey, changes)
			if err != nil {
				return fmt.Errorf("database insert: %w", err)
			}
		case pbddatabase.TableChange_UPDATE:
			err := s.DBLoader.Update(change.Table, primaryKey, changes)
			if err != nil {
				return fmt.Errorf("database update: %w", err)
			}
		case pbddatabase.TableChange_DELETE:
			err := s.DBLoader.Delete(change.Table, primaryKey)
			if err != nil {
				return fmt.Errorf("database delete: %w", err)
			}
		default:
			//case database.TableChange_UNSET:
		}
	}
	return nil
}

func (s *PostgresSinker) handleBlockScopeData(ctx context.Context, cursor *sink.Cursor, data *pbsubstreams.BlockScopedData) error {
	for _, output := range data.Outputs {
		if output.Name != s.OutputModuleName {
			continue
		}

		dbChanges := &pbddatabase.DatabaseChanges{}
		err := proto.Unmarshal(output.GetMapOutput().GetValue(), dbChanges)
		if err != nil {
			return fmt.Errorf("unmarshal database changes: %w", err)
		}

		err = s.applyDatabaseChanges(dbChanges)
		if err != nil {
			return fmt.Errorf("apply database changes: %w", err)
		}
	}

	if cursor.Block.Num()%BLOCK_PROGRESS == 0 {
		flushStart := time.Now()
		if err := s.DBLoader.Flush(ctx, hex.EncodeToString(s.OutputModuleHash), cursor); err != nil {
			return fmt.Errorf("failed to roll: %w", err)
		}
		flushDuration := time.Since(flushStart)
		FlushCount.Inc()
		FlushedEntriesCount.AddUint64(s.DBLoader.EntriesCount)
		FlushDuration.AddInt(int(flushDuration.Nanoseconds()))
	}

	return nil
}
