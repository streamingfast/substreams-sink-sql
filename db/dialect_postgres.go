package db

import (
	"context"
	"fmt"

	"github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
)

type postgresDialect struct{}

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
