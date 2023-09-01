package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

func (l *Loader) Flush(ctx context.Context, outputModuleHash string, cursor *sink.Cursor) (err error) {
	ctx = clickhouse.Context(context.Background(), clickhouse.WithStdAsync(false))
	startAt := time.Now()

	tx, err := l.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to being db transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if err := tx.Rollback(); err != nil {
				l.logger.Warn("failed to rollback transaction", zap.Error(err))
			}
		}
	}()

	var entryCount int
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		tableName := entriesPair.Key
		entries := entriesPair.Value

		if l.tracer.Enabled() {
			l.logger.Debug("flushing table entries", zap.String("table_name", tableName), zap.Int("entry_count", entries.Len()))
		}

		for entryPair := entries.Oldest(); entryPair != nil; entryPair = entryPair.Next() {
			entry := entryPair.Value

			query, err := entry.query(l.getDialect())
			if err != nil {
				return fmt.Errorf("failed to get query: %w", err)
			}

			if l.tracer.Enabled() {
				l.logger.Debug("adding query from operation to transaction", zap.Stringer("op", entry), zap.String("query", query))
			}

			if _, err := tx.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("executing query %q: %w", query, err)
			}
		}

		entryCount += entries.Len()
	}

	entryCount += 1
	if err := l.UpdateCursor(ctx, tx, outputModuleHash, cursor); err != nil {
		return fmt.Errorf("update cursor: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit db transaction: %w", err)
	}
	l.reset()

	l.logger.Debug("flushed table(s) entries", zap.Int("table_count", l.entries.Len()), zap.Int("entry_count", entryCount), zap.Duration("took", time.Since(startAt)))
	return nil
}

func (l *Loader) reset() {
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		l.entries.Set(entriesPair.Key, NewOrderedMap[string, *Operation]())
	}
}
