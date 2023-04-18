package db

import (
	"context"
	"fmt"
	"reflect"
	"time"

	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

func (l *Loader) Flush(ctx context.Context, moduleHash string, cursor *sink.Cursor) (err error) {
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
			query, err := entry.query(l.getType)
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
	cursorQuery := l.UpdateCursorQuery(moduleHash, cursor)
	if _, err := tx.ExecContext(ctx, cursorQuery); err != nil {
		return fmt.Errorf("executing query %q: %w", cursorQuery, err)
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

func (l *Loader) getType(tableName string, columnName string) (reflect.Type, error) {
	if t, found := l.tables[tableName][columnName]; found {
		return t, nil
	}
	return nil, fmt.Errorf("cannot find type of column %q for table %q", columnName, tableName)
}
