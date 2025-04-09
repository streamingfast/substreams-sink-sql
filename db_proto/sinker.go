package db_proto

import (
	"context"
	"fmt"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	sink "github.com/streamingfast/substreams-sink"
	sql "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/stats"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
)

type Sinker struct {
	*sink.Sinker
	db             *sql.Database
	useTransaction bool
	blockBatchSize uint64

	stats  *stats.Stats
	logger *zap.Logger
}

func NewSinker(logger *zap.Logger, sink *sink.Sinker, db *sql.Database, useTransaction bool, blockBatchSize int, stats *stats.Stats) *Sinker {
	return &Sinker{
		db:             db,
		useTransaction: useTransaction,
		blockBatchSize: uint64(blockBatchSize),
		stats:          stats,
		Sinker:         sink,
		logger:         logger,
	}
}

func (s *Sinker) Run(ctx context.Context) error {
	cursor, err := sql.FetchCursor(s.db.Db, s.db.Schema)
	if err != nil {
		return fmt.Errorf("fetch cursor: %w", err)
	}

	//clean up the mess from running without a transaction
	err = s.db.HandleBlocksUndo(cursor.Block().Num(), cursor)
	if err != nil {
		return fmt.Errorf("handle blocks undo from %d : %w", cursor.Block().Num(), err)
	}

	s.logger.Info("fetched cursor", zap.Uint64("block_num", cursor.Block().Num()))

	s.stats.LastBlockProcessAt = time.Now()
	s.Sinker.Run(ctx, cursor, s)
	return nil
}

type Holder struct {
	output *pbsubstreamsrpc.MapModuleOutput
	data   *pbsubstreamsrpc.BlockScopedData
	isLive *bool
	cursor *sink.Cursor
}

var holding []*Holder

func (s *Sinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) (err error) {
	startAt := time.Now()
	defer func() {
		s.stats.LastBlockProcessAt = time.Now()
		s.stats.BlockProcessingDuration.Add(time.Since(startAt))
		s.stats.TotalProcessingDuration += time.Since(startAt)
	}()

	s.stats.WaitDurationBetweenBlocks.Add(time.Since(s.stats.LastBlockProcessAt))
	s.stats.BlockCount++

	output := data.Output
	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	if s.blockBatchSize == 1 {

	}
	holder := &Holder{
		output: output,
		data:   data,
		isLive: isLive,
		cursor: cursor,
	}
	holding = append(holding, holder)
	if data.Clock.Number%s.blockBatchSize == 0 || s.blockBatchSize == 1 {
		if s.useTransaction {
			if err := s.db.BeginTransaction(); err != nil {
				return fmt.Errorf("begin tx: %w", err)
			}
		}

		for _, h := range holding {
			err = s.processHolder(h)
			if err != nil {
				if s.useTransaction {
					s.db.RollbackTransaction()
				}
				return fmt.Errorf("process holder: %w", err)
			}
		}

		err = s.db.InsertCursor(cursor)
		if err != nil {
			return fmt.Errorf("inserting cursor: %w", err)
		}

		if s.useTransaction {
			if err := s.db.CommitTransaction(); err != nil {
				return fmt.Errorf("commit tx: %w", err)
			}
		}
		holding = []*Holder{}
	}

	return nil
}

func (s *Sinker) processHolder(h *Holder) (err error) {
	if len(h.output.GetMapOutput().GetValue()) == 0 {
		return nil
	}

	unmarshalStartAt := time.Now()
	md := s.db.RootMessageDescriptor
	dm := dynamic.NewMessage(md)
	err = dm.Unmarshal(h.data.Output.GetMapOutput().GetValue())
	if err != nil {
		return fmt.Errorf("unmarshaling message: %w", err)
	}
	s.stats.UnmarshallingDuration.Add(time.Since(unmarshalStartAt))

	err = s.db.ProcessMessage(dm, h.data.Clock.Number, h.data.Clock.Id, h.data.Clock.Timestamp.AsTime(), s.stats)
	if err != nil {
		return fmt.Errorf("process entity: %w", err)
	}

	return nil
}

func (s *Sinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) (err error) {
	lastValidBlockNum := undoSignal.LastValidBlock.Number

	s.logger.Info("Handling undo block signal", zap.Stringer("block", cursor.Block()), zap.Stringer("cursor", cursor))

	err = s.db.HandleBlocksUndo(lastValidBlockNum, cursor)
	if err != nil {
		return fmt.Errorf("handle blocks undo from %d : %w", lastValidBlockNum, err)
	}

	return nil
}
