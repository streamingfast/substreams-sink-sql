package db

import (
	"context"
	"database/sql"
	"fmt"

	sink "github.com/streamingfast/substreams-sink"
)

type UnknownDriverError struct {
	Driver string
}

// Error returns a formatted string description.
func (e UnknownDriverError) Error() string {
	return fmt.Sprintf("unknown database driver: %s", e.Driver)
}

type dialect interface {
	GetCreateCursorQuery(schema string) string
	ExecuteSetupScript(ctx context.Context, l *Loader, schemaSql string) error
	DriverSupportRowsAffected() bool
	GetUpdateCursorQuery(table, moduleHash string, cursor *sink.Cursor, block_num uint64, block_id string) string
	ParseDatetimeNormalization(value string) string
	Flush(tx *sql.Tx, ctx context.Context, l *Loader, outputModuleHash string, cursor *sink.Cursor) (int, error)
	OnlyInserts() bool
}

var driverDialect = map[string]dialect{
	"*pq.Driver":            postgresDialect{},   // github.com/lib/pq
	"*clickhouse.stdDriver": clickhouseDialect{}, // github.com/clickhouse-go/v2
}
