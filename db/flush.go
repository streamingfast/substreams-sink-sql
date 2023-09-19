package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

func (l *Loader) Flush(ctx context.Context, outputModuleHash string, cursor *sink.Cursor) (rowFlushedCount int, err error) {
	ctx = clickhouse.Context(context.Background(), clickhouse.WithStdAsync(false))

	startAt := time.Now()
	tx, err := l.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to being db transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if err := tx.Rollback(); err != nil {
				l.logger.Warn("failed to rollback transaction", zap.Error(err))
			}
		}
	}()

	rowFlushedCount, err = l.getDialect().Flush(tx, ctx, l, outputModuleHash, cursor)
	if err != nil {
		return 0, fmt.Errorf("dialect flush: %w", err)
	}

	rowFlushedCount += 1
	if err := l.UpdateCursor(ctx, tx, outputModuleHash, cursor); err != nil {
		return 0, fmt.Errorf("update cursor: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit db transaction: %w", err)
	}
	l.reset()

	// We add + 1 to the table count because the `cursors` table is an implicit table
	l.logger.Debug("flushed table(s) rows to database", zap.Int("table_count", l.entries.Len()+1), zap.Int("row_count", rowFlushedCount), zap.Duration("took", time.Since(startAt)))
	return rowFlushedCount, nil
}

func (l *Loader) reset() {
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		l.entries.Set(entriesPair.Key, NewOrderedMap[string, *Operation]())
	}
}
