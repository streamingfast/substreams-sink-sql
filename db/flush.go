package db

import (
	"context"
	"fmt"
	"reflect"

	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

func (l *Loader) Flush(ctx context.Context, moduleHash string, cursor *sink.Cursor) (err error) {
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

	for tableName, entries := range l.entries {
		l.logger.Info("flushing table entries", zap.String("table_name", tableName), zap.Int("entry_count", len(entries)))
		for _, entry := range entries {
			query, err := entry.query(l.getType)
			if err != nil {
				return fmt.Errorf("failed to get query: %w", err)
			}

			if _, err := tx.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("executing query:\n`%s`\n err: %w", query, err)
			}
		}
	}

	cursorQuery := l.UpdateCursorQuery(moduleHash, cursor)
	if _, err := tx.ExecContext(ctx, cursorQuery); err != nil {
		return fmt.Errorf("executing query:\n`%s`\n err: %w", cursorQuery, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit db transaction: %w", err)
	}
	return nil
}

func (l *Loader) Reset() {
	for tableName, _ := range l.entries {
		l.entries[tableName] = map[string]*Operation{}
	}
	l.EntriesCount = 0
}

func (l *Loader) getType(tableName string, columnName string) (reflect.Type, error) {
	if t, found := l.tables[tableName][columnName]; found {
		return t, nil
	}
	return nil, fmt.Errorf("cannot find type of column %q for table %q", columnName, tableName)
}
