package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
)

type clickhouseDialect struct{}

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
