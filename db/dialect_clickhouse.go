package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

type clickhouseDialect struct{}

// Clickhouse should be used to insert a lot of data in batches. The current official clickhouse
// driver doesn't support Transactions for multiple tables. The only way to add in batches is 
// creating a transaction for a table, adding all rows and commiting it.
//
// That's why two different Flush() functions are needed depending on the dialect.
func (d clickhouseDialect) Flush(tx *sql.Tx, ctx context.Context, l *Loader, outputModuleHash string, cursor *sink.Cursor) (int, error) {
	var entryCount int
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		tableName := entriesPair.Key
		entries := entriesPair.Value
		tx, err := l.DB.BeginTx(ctx, nil)
		if err != nil {
			return entryCount, fmt.Errorf("failed to begin db transaction")
		}

		if l.tracer.Enabled() {
			l.logger.Debug("flushing table entries", zap.String("table_name", tableName), zap.Int("entry_count", entries.Len()))
		}
		info := l.tables[tableName]
		columns := make([]string, 0, len(info.columnsByName))
		for column := range info.columnsByName {
			columns = append(columns, column)
		}
		sort.Strings(columns)
		query := fmt.Sprintf(
			"INSERT INTO %s.%s (%s)",
			EscapeIdentifier(l.schema),
			EscapeIdentifier(tableName),
			strings.Join(columns, ","))
		batch, err := tx.Prepare(query)
		if err != nil {
			return entryCount, fmt.Errorf("failed to prepare insert into %q: %w", tableName, err)
		}
		for entryPair := entries.Oldest(); entryPair != nil; entryPair = entryPair.Next() {
			entry := entryPair.Value

			if err != nil {
				return entryCount, fmt.Errorf("failed to get query: %w", err)
			}

			if l.tracer.Enabled() {
				l.logger.Debug("adding query from operation to transaction", zap.Stringer("op", entry), zap.String("query", query))
			}

			values, err := entry.getValues()
			if err != nil {
				return entryCount, fmt.Errorf("failed to get values: %w", err)
			}

			if _, err := batch.ExecContext(ctx, values...); err != nil {
				return entryCount, fmt.Errorf("executing for entry %q: %w", values, err)
			}
		}

		if err := tx.Commit(); err != nil {
			return entryCount, fmt.Errorf("failed to commit db transaction: %w", err)
		}
		entryCount += entries.Len()
	}

	return entryCount, nil
}

func (d clickhouseDialect) GetCreateCursorQuery(schema string) string {
	return fmt.Sprintf(cli.Dedent(`
	CREATE TABLE IF NOT EXISTS %s.%s
	(
    id         String,
		cursor     String,
		block_num  Int64,
		block_id   String
	) Engine = ReplacingMergeTree() ORDER BY id;
	`), EscapeIdentifier(schema), EscapeIdentifier("cursors"))
}

func (d clickhouseDialect) ExecuteSetupScript(ctx context.Context, l *Loader, schemaSql string) error {
	for _, query := range strings.Split(schemaSql, ";") {
		if len(strings.TrimSpace(query)) == 0 {
			continue
		}
		if _, err := l.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("exec schema: %w", err)
		}
	}
	return nil
}

func (d clickhouseDialect) GetUpdateCursorQuery(table, moduleHash string, cursor *sink.Cursor, block_num uint64, block_id string) string {
	return query(`
			INSERT INTO %s (id, cursor, block_num, block_id) values ('%s', '%s', %d, '%s')
	`, table, moduleHash, cursor, block_num, block_id)
}

func (d clickhouseDialect) ParseDatetimeNormalization(value string) string {
	return fmt.Sprintf("parseDateTimeBestEffort(%s)", escapeStringValue(value))
}

func (d clickhouseDialect) DriverSupportRowsAffected() bool {
	return false
}

func (d clickhouseDialect) OnlyInserts() bool {
	return true
}
