package db

import (
	"context"
	"fmt"
	"time"

	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

func (l *Loader) Flush(ctx context.Context, outputModuleHash string, cursor *sink.Cursor) (err error) {
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
	for tableName, entries := range l.entries {
		if l.tracer.Enabled() {
			l.logger.Debug("flushing table entries", zap.String("table_name", tableName), zap.Int("entry_count", len(entries)))
		}

		for _, entry := range entries {
			query, err := entry.query()
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

		entryCount += len(entries)
	}

	entryCount += 1
	if err := l.UpdateCursor(ctx, tx, outputModuleHash, cursor); err != nil {
		return fmt.Errorf("update cursor: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit db transaction: %w", err)
	}
	l.reset()

	l.logger.Debug("flushed table(s) entries", zap.Int("table_count", len(l.entries)), zap.Int("entry_count", entryCount), zap.Duration("took", time.Since(startAt)))
	return nil
}

func (l *Loader) reset() {
	for tableName := range l.entries {
		l.entries[tableName] = map[string]*Operation{}
	}
}
