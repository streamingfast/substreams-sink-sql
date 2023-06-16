package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jimsmart/schema"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	orderedmap "github.com/wk8/go-ordered-map/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Make the typing a bit easier
type OrderedMap[K comparable, V any] struct {
	*orderedmap.OrderedMap[K, V]
}

func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{OrderedMap: orderedmap.New[K, V]()}
}

type CursorError struct {
	error
}

type Loader struct {
	*sql.DB

	database     string
	schema       string
	entries      *OrderedMap[string, *OrderedMap[string, *Operation]]
	entriesCount uint64
	tables       map[string]*TableInfo
	cursorTable  *TableInfo

	flushInterval      time.Duration
	moduleMismatchMode OnModuleHashMismatch

	logger *zap.Logger
	tracer logging.Tracer
}

func NewLoader(
	psqlDsn string,
	flushInterval time.Duration,
	moduleMismatchMode OnModuleHashMismatch,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*Loader, error) {
	dsn, err := parseDSN(psqlDsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	db, err := sql.Open("postgres", dsn.connString())
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	logger.Debug("created new DB loader",
		zap.Duration("flush_interval", flushInterval),
		zap.String("database", dsn.database),
		zap.String("schema", dsn.schema),
		zap.String("host", dsn.host),
		zap.Int64("port", dsn.port),
		zap.Stringer("on_module_hash_mismatch", moduleMismatchMode),
	)

	return &Loader{
		DB:                 db,
		database:           dsn.database,
		schema:             dsn.schema,
		entries:            NewOrderedMap[string, *OrderedMap[string, *Operation]](),
		tables:             map[string]*TableInfo{},
		flushInterval:      flushInterval,
		moduleMismatchMode: moduleMismatchMode,
		logger:             logger,
		tracer:             tracer,
	}, nil
}

func (l *Loader) EntriesCount() uint64 {
	return l.entriesCount
}

func (l *Loader) FlushInterval() time.Duration {
	return l.flushInterval
}

func (l *Loader) LoadTables() error {
	schemaTables, err := schema.Tables(l.DB)
	if err != nil {
		return fmt.Errorf("retrieving table and schema: %w", err)
	}

	seenCursorTable := false
	for schemaTableName, columns := range schemaTables {
		schemaName := schemaTableName[0]
		tableName := schemaTableName[1]
		l.logger.Debug("processing schema's table",
			zap.String("schema_name", schemaName),
			zap.String("table_name", tableName),
		)

		if schemaName != l.schema {
			continue
		}

		if tableName == "cursors" {
			if err := l.validateCursorTables(columns); err != nil {
				return fmt.Errorf("invalid cursors table: %w", err)
			}

			seenCursorTable = true
		}

		columnByName := make(map[string]*ColumnInfo, len(columns))
		for _, f := range columns {
			columnByName[f.Name()] = &ColumnInfo{
				name:             f.Name(),
				escapedName:      escapeIdentifier(f.Name()),
				databaseTypeName: f.DatabaseTypeName(),
				scanType:         f.ScanType(),
			}
		}

		key, err := schema.PrimaryKey(l.DB, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("get primary key: %w", err)
		}

		l.tables[tableName], err = NewTableInfo(schemaName, tableName, key, columnByName)
		if err != nil {
			return fmt.Errorf("invalid table: %w", err)
		}
	}

	if !seenCursorTable {
		return &CursorError{fmt.Errorf(`%s."cursors" table is not found`, escapeIdentifier(l.schema))}
	}
	l.cursorTable = l.tables["cursors"]

	return nil
}

func (l *Loader) validateCursorTables(columns []*sql.ColumnType) (err error) {
	if len(columns) != 4 {
		return &CursorError{fmt.Errorf("table requires 4 columns ('id', 'cursor', 'block_num', 'block_id')")}
	}
	columnsCheck := map[string]string{
		"block_num": "int64",
		"block_id":  "string",
		"cursor":    "string",
		"id":        "string",
	}
	for _, f := range columns {
		columnName := f.Name()
		if _, found := columnsCheck[columnName]; !found {
			return &CursorError{fmt.Errorf("unexpected column %q in cursors table", columnName)}
		}
		expectedType := columnsCheck[columnName]
		actualType := f.ScanType().Kind().String()
		if expectedType != actualType {
			return &CursorError{fmt.Errorf("column %q has invalid type, expected %q has %q", columnName, expectedType, actualType)}
		}
		delete(columnsCheck, columnName)
	}
	if len(columnsCheck) != 0 {
		for k := range columnsCheck {
			return &CursorError{fmt.Errorf("missing column %q from cursors", k)}
		}
	}
	key, err := schema.PrimaryKey(l.DB, l.schema, "cursors")
	if err != nil {
		return &CursorError{fmt.Errorf("failed getting primary key: %w", err)}
	}
	if len(key) == 0 {
		return &CursorError{fmt.Errorf("primary key not found: %w", err)}
	}
	if key[0] != "id" {
		return &CursorError{fmt.Errorf("column 'id' should be primary key not %q", key[0])}
	}
	return nil
}

// GetIdentifier returns <database>/<schema> suitable for user presentation
func (l *Loader) GetIdentifier() string {
	return fmt.Sprintf("%s/%s", l.database, l.schema)
}

func (l *Loader) GetAvailableTablesInSchema() string {
	tables := make([]string, len(l.tables))
	i := 0
	for table := range l.tables {
		tables[i] = table
		i++
	}

	return strings.Join(tables, ", ")
}

func (l *Loader) GetDatabase() string {
	return l.database
}

func (l *Loader) GetSchema() string {
	return l.schema
}

func (l *Loader) HasTable(tableName string) bool {
	if _, found := l.tables[tableName]; found {
		return true
	}
	return false
}

func (l *Loader) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("entries_count", l.entriesCount)
	return nil
}

func (l *Loader) Setup(ctx context.Context, schemaFile string) error {
	b, err := os.ReadFile(schemaFile)
	if err != nil {
		return fmt.Errorf("read schema file: %w", err)
	}

	schemaSql := string(b)
	if _, err := l.ExecContext(ctx, schemaSql); err != nil {
		return fmt.Errorf("exec schema: %w", err)
	}

	err = l.setupCursorTable(ctx)
	if err != nil {
		return fmt.Errorf("setup cursor table: %w", err)
	}

	return nil
}

func (l *Loader) setupCursorTable(ctx context.Context) error {
	_, err := l.ExecContext(ctx, l.GetCreateCursorsTableSQL())

	if err != nil {
		return fmt.Errorf("creating cursor table: %w", err)
	}

	return nil
}

func (l *Loader) GetCreateCursorsTableSQL() string {
	return fmt.Sprintf(cli.Dedent(`
		create table if not exists %s.%s
		(
			id         text not null constraint cursor_pk primary key,
			cursor     text,
			block_num  bigint,
			block_id   text
		);
	`), escapeIdentifier(l.schema), escapeIdentifier("cursors"))
}
