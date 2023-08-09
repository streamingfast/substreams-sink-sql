package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

type postgresDialect struct{}

func (d postgresDialect) Flush(tx *sql.Tx, ctx context.Context, l *Loader, outputModuleHash string, cursor *sink.Cursor) (int, error) {
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
				return 0, fmt.Errorf("failed to get query: %w", err)
			}

			if l.tracer.Enabled() {
				l.logger.Debug("adding query from operation to transaction", zap.Stringer("op", entry), zap.String("query", query))
			}

			if _, err := tx.ExecContext(ctx, query); err != nil {
				return 0, fmt.Errorf("executing query %q: %w", query, err)
			}
		}
		entryCount += entries.Len()
	}

	return entryCount, nil
}

func (d postgresDialect) GetCreateCursorQuery(schema string) string {
	return fmt.Sprintf(cli.Dedent(`
		create table if not exists %s.%s
		(
			id         text not null constraint cursor_pk primary key,
			cursor     text,
			block_num  bigint,
			block_id   text
		);
		`), EscapeIdentifier(schema), EscapeIdentifier("cursors"))
}

func (d postgresDialect) ExecuteSetupScript(ctx context.Context, l *Loader, schemaSql string) error {
	if _, err := l.ExecContext(ctx, schemaSql); err != nil {
		return fmt.Errorf("exec schema: %w", err)
	}
	return nil
}

func (d postgresDialect) GetUpdateCursorQuery(table, moduleHash string, cursor *sink.Cursor, block_num uint64, block_id string) string {
	return query(`
		UPDATE %s set cursor = '%s', block_num = %d, block_id = '%s' WHERE id = '%s';
	`, table, cursor, block_num, block_id, moduleHash)
}

func (d postgresDialect) ParseDatetimeNormalization(value string) string {
	return escapeStringValue(value)
}

func (d postgresDialect) DriverSupportRowsAffected() bool {
	return true
}

func (d postgresDialect) OnlyInserts() bool {
	return false
}
