package data

import (
	"context"
	"fmt"
	"time"

	sink "github.com/streamingfast/substreams-sink"
	sql "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	v1 "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

type Sinker struct {
	logger *zap.Logger
	*sink.Sinker
	db            *sql.Database
	lastClock     *v1.Clock
	blockSecCount int64
}

func NewSinker(logger *zap.Logger, sink *sink.Sinker, db *sql.Database) *Sinker {
	return &Sinker{
		logger: logger,
		Sinker: sink,
		db:     db,
	}
}

func (s *Sinker) Run(ctx context.Context) error {

	go func() {
		for {
			time.Sleep(5 * time.Second)
			if s.lastClock != nil {
				s.logger.Info("progress_block", zap.Stringer("block", s.lastClock))
			}
		}
	}()

	cursor, err := sql.FetchCursor(s.db.Db, s.db.Schema)
	if err != nil {
		return fmt.Errorf("fetch cursor: %w", err)
	}

	s.logger.Info("fetched cursor", zap.Uint64("block_num", cursor.Block().Num()))

	s.Sinker.Run(ctx, cursor, s)
	return nil
}

func (s *Sinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) (err error) {
	s.blockSecCount++
	s.lastClock = data.Clock

	output := data.Output
	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	if len(output.GetMapOutput().GetValue()) == 0 {
		return nil
	}

	err = s.db.ProcessEntity(output.GetMapOutput().GetValue(), data.Clock.Number, data.Clock.Id, data.Clock.Timestamp.AsTime(), cursor)
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
