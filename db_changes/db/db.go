package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/jimsmart/schema"
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

type SystemTableError struct {
	error
}

type Loader struct {
	*sql.DB

	entries      *OrderedMap[string, *OrderedMap[string, *Operation]]
	entriesCount uint64
	tables       map[string]*TableInfo
	cursorTable  *TableInfo

	handleReorgs            bool
	batchBlockFlushInterval int
	batchRowFlushInterval   int
	liveBlockFlushInterval  int
	moduleMismatchMode      OnModuleHashMismatch

	dialect Dialect

	logger *zap.Logger
	tracer logging.Tracer

	testTx *TestTx // used for testing: if non-nil, 'loader.BeginTx()' will return this object instead of a real *sql.Tx
	dsn    *DSN

	// Postgres insert batching (runtime-configurable; defaults imply disabled)
	pgInsertBatchMode string // off|values|unnest
	pgInsertBatchSize int    // >0 when enabled
	pgInsertOnly      bool   // assert-only mode; if true, non-insert ops should be rejected or cause fallback
}

func NewLoader(
	dsn *DSN,
	cursorTableName string, historyTableName string, clickhouseCluster string,
	batchBlockFlushInterval int,
	batchRowFlushInterval int,
	liveBlockFlushInterval int,
	OnModuleHashMismatch string,
	handleReorgs *bool,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*Loader, error) {

	sqlDB, err := sql.Open(dsn.Driver(), dsn.ConnString())
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	dialect, err := newDialect(sqlDB.Driver(), dsn.Schema(), cursorTableName, historyTableName, clickhouseCluster)
	if err != nil {
		return nil, fmt.Errorf("get dialect: %w", err)
	}

	moduleMismatchMode, err := ParseOnModuleHashMismatch(OnModuleHashMismatch)
	if err != nil {
		return nil, fmt.Errorf("parse on module hash mismatch: %w", err)
	}

	l := &Loader{
		DB:                      sqlDB,
		dsn:                     dsn,
		entries:                 NewOrderedMap[string, *OrderedMap[string, *Operation]](),
		tables:                  map[string]*TableInfo{},
		batchBlockFlushInterval: batchBlockFlushInterval,
		batchRowFlushInterval:   batchRowFlushInterval,
		liveBlockFlushInterval:  liveBlockFlushInterval,
		moduleMismatchMode:      moduleMismatchMode,
		dialect:                 dialect,
		logger:                  logger,
		tracer:                  tracer,
	}

	if handleReorgs == nil {
		// automatic detection
		l.handleReorgs = !l.dialect.OnlyInserts()
	} else {
		l.handleReorgs = *handleReorgs
	}

	if l.handleReorgs && l.dialect.OnlyInserts() {
		return nil, fmt.Errorf("driver %s does not support reorg handling. You must use set a non-zero undo-buffer-size", sqlDB.Driver())
	}

	logger.Info("created new DB loader",
		zap.Int("batch_block_flush_interval", batchBlockFlushInterval),
		zap.Int("batch_row_flush_interval", batchRowFlushInterval),
		zap.Int("live_block_flush_interval", liveBlockFlushInterval),
		zap.Stringer("on_module_hash_mismatch", moduleMismatchMode),
		zap.Bool("handle_reorgs", l.handleReorgs),
		zap.String("dialect", fmt.Sprintf("%T", l.dialect)),
	)

	return l, nil
}

func newDialect(driver driver.Driver, schemaName string, cursorTableName string, historyTableName string, clickHouseClusterName string) (Dialect, error) {
	driverType := fmt.Sprintf("%T", driver)
	switch driverType {
	case "*pq.Driver":
		return NewPostgresDialect(schemaName, cursorTableName, historyTableName), nil
	case "*clickhouse.stdDriver":
		return NewClickhouseDialect(schemaName, cursorTableName, clickHouseClusterName), nil
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driverType)
	}
}

type Tx interface {
	Rollback() error
	Commit() error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (l *Loader) Begin() (Tx, error) {
	return l.BeginTx(context.Background(), nil)
}

func (l *Loader) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	if l.testTx != nil {
		return l.testTx, nil
	}
	return l.DB.BeginTx(ctx, opts)
}

func (l *Loader) BatchBlockFlushInterval() int {
	return l.batchBlockFlushInterval
}

func (l *Loader) LiveBlockFlushInterval() int {
	return l.liveBlockFlushInterval
}

func (l *Loader) FlushNeeded() bool {
	totalRows := 0
	// todo keep a running count when inserting/deleting rows directly
	for pair := l.entries.Oldest(); pair != nil; pair = pair.Next() {
		totalRows += pair.Value.Len()
	}
	return totalRows > l.batchRowFlushInterval
}

// PgInsertBatchMode returns the configured Postgres batch mode ("off" if unset).
func (l *Loader) PgInsertBatchMode() string {
	if l.pgInsertBatchMode == "" {
		return "off"
	}
	return l.pgInsertBatchMode
}

// PgInsertBatchSize returns the configured batch size (0 means unset/disabled).
func (l *Loader) PgInsertBatchSize() int {
	return l.pgInsertBatchSize
}

// PgInsertOnly indicates whether insert-only assertions are enabled.
func (l *Loader) PgInsertOnly() bool {
	return l.pgInsertOnly
}

// ConfigurePgInsertBatching sets Postgres insert batching configuration on the loader.
// mode should be one of: "off", "values", "unnest". size <= 0 disables batching.
func (l *Loader) ConfigurePgInsertBatching(mode string, size int, insertOnly bool) {
	l.pgInsertBatchMode = mode
	l.pgInsertBatchSize = size
	l.pgInsertOnly = insertOnly
}

// getTablesFromSchema returns table information similar to schema.Tables()
// but only inspects tables in the specified schema to avoid issues with database extensions
func (l *Loader) getTablesFromSchema(schemaName string) (map[[2]string][]*sql.ColumnType, error) {
	// Only get tables from the specified schema
	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		AND table_schema = $1
		ORDER BY table_schema, table_name
	`

	rows, err := l.DB.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	result := make(map[[2]string][]*sql.ColumnType)

	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("scanning table row: %w", err)
		}

		// Get column information for this table
		columns, err := l.getTableColumns(schemaName, tableName)
		if err != nil {
			l.logger.Warn("failed to get columns for table, skipping",
				zap.String("schema", schemaName),
				zap.String("table", tableName),
				zap.Error(err),
			)
			continue
		}

		key := [2]string{schemaName, tableName}
		result[key] = columns
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating table rows: %w", err)
	}

	return result, nil
}

// getTableColumns returns column information for a specific table
func (l *Loader) getTableColumns(schemaName, tableName string) ([]*sql.ColumnType, error) {
	// Use a simple query to get column information
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE 1=0",
		EscapeIdentifier(schemaName),
		EscapeIdentifier(tableName))

	rows, err := l.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("querying table structure: %w", err)
	}
	defer rows.Close()

	return rows.ColumnTypes()
}

// fetchColumnPgTypes returns a mapping of column name to PostgreSQL type name (pg_type.typname),
// which properly reflects user-defined types (e.g., enums) and array types (leading underscore).
// Example: bigint[] -> _int8, enum call_type -> call_type, enum[] -> _call_type.
func (l *Loader) fetchColumnPgTypes(schemaName, tableName string) (map[string]string, error) {
	query := `
        SELECT a.attname, t.typname
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_type t ON t.oid = a.atttypid
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum`

	rows, err := l.DB.Query(query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying pg types for %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var col string
		var typ string
		if err := rows.Scan(&col, &typ); err != nil {
			return nil, fmt.Errorf("scanning pg types row: %w", err)
		}
		out[col] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pg types rows: %w", err)
	}
	return out, nil
}

// fetchColumnNullabilityDefaults returns per-column nullability and default expression info.
// defaultExpr is returned as a raw SQL expression via pg_get_expr(adbin, adrelid).
func (l *Loader) fetchColumnNullabilityDefaults(schemaName, tableName string) (map[string]struct {
	Nullable    bool
	HasDefault  bool
	DefaultExpr string
}, error) {
	query := `
        SELECT a.attname,
               NOT a.attnotnull AS nullable,
               (ad.adbin IS NOT NULL) AS has_default,
               pg_get_expr(ad.adbin, ad.adrelid) AS default_expr
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum`

	rows, err := l.DB.Query(query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying nullability/defaults for %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	out := make(map[string]struct {
		Nullable    bool
		HasDefault  bool
		DefaultExpr string
	})
	for rows.Next() {
		var col string
		var nullable bool
		var hasDefault bool
		var defaultExpr sql.NullString
		if err := rows.Scan(&col, &nullable, &hasDefault, &defaultExpr); err != nil {
			return nil, fmt.Errorf("scanning nullability/default row: %w", err)
		}
		expr := ""
		if defaultExpr.Valid {
			expr = defaultExpr.String
		}
		out[col] = struct {
			Nullable    bool
			HasDefault  bool
			DefaultExpr string
		}{Nullable: nullable, HasDefault: hasDefault, DefaultExpr: expr}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating nullability/default rows: %w", err)
	}
	return out, nil
}

func (l *Loader) LoadTables(schemaName string, cursorTableName string, historyTableName string) error {
	schemaTables, err := l.getTablesFromSchema(schemaName)
	if err != nil {
		return fmt.Errorf("retrieving table and schemaName: %w", err)
	}

	seenCursorTable := false
	seenHistoryTable := false
	for schemaTableName, columns := range schemaTables {
		tableName := schemaTableName[1]
		l.logger.Debug("processing schemaName's table",
			zap.String("schema_name", schemaName),
			zap.String("table_name", tableName),
		)

		if schemaTableName[0] != schemaName {
			continue
		}

		if tableName == cursorTableName {
			if err := l.validateCursorTables(columns, schemaName, cursorTableName); err != nil {
				return fmt.Errorf("invalid cursors table: %w", err)
			}

			seenCursorTable = true
		}
		if tableName == historyTableName {
			seenHistoryTable = true
		}

		columnByName := make(map[string]*ColumnInfo, len(columns))
		// Try to enrich with real PostgreSQL type names (handles enums/domains/arrays)
		pgTypes, _ := l.fetchColumnPgTypes(schemaName, tableName)
		// Also fetch nullability/defaults metadata for 11c default inlining
		ndMeta, _ := l.fetchColumnNullabilityDefaults(schemaName, tableName)
		for _, f := range columns {
			dbType := f.DatabaseTypeName()
			if real, ok := pgTypes[f.Name()]; ok && real != "" {
				dbType = real
			}
			info := &ColumnInfo{
				name:             f.Name(),
				escapedName:      EscapeIdentifier(f.Name()),
				databaseTypeName: dbType,
				scanType:         f.ScanType(),
			}
			if meta, ok := ndMeta[f.Name()]; ok {
				info.nullable = meta.Nullable
				info.hasDefault = meta.HasDefault
				info.defaultExpr = meta.DefaultExpr
			}
			columnByName[f.Name()] = info
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
		return &SystemTableError{fmt.Errorf(`%s.%s table is not found`, EscapeIdentifier(schemaName), cursorTableName)}
	}
	if l.handleReorgs && !seenHistoryTable {
		return &SystemTableError{fmt.Errorf("%s.%s table is not found and reorgs handling is enabled", EscapeIdentifier(schemaName), historyTableName)}
	}

	l.cursorTable = l.tables[cursorTableName]

	return nil
}

func (l *Loader) validateCursorTables(columns []*sql.ColumnType, schemaName string, cursorTableName string) (err error) {
	if len(columns) != 4 {
		return &SystemTableError{fmt.Errorf("table requires 4 columns ('id', 'cursor', 'block_num', 'block_id')")}
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
			return &SystemTableError{fmt.Errorf("unexpected column %q in cursors table", columnName)}
		}
		expectedType := columnsCheck[columnName]
		actualType := f.ScanType().Kind().String()
		if expectedType != actualType {
			return &SystemTableError{fmt.Errorf("column %q has invalid type, expected %q has %q", columnName, expectedType, actualType)}
		}
		delete(columnsCheck, columnName)
	}
	if len(columnsCheck) != 0 {
		for k := range columnsCheck {
			return &SystemTableError{fmt.Errorf("missing column %q from cursors", k)}
		}
	}
	key, err := schema.PrimaryKey(l.DB, schemaName, cursorTableName)
	if err != nil {
		return &SystemTableError{fmt.Errorf("failed getting primary key: %w", err)}
	}
	if len(key) == 0 {
		return &SystemTableError{fmt.Errorf("primary key not found: %w", err)}
	}
	if key[0] != "id" {
		return &SystemTableError{fmt.Errorf("column 'id' should be primary key not %q", key[0])}
	}
	return nil
}

func (l *Loader) GetColumnsForTable(name string) []string {
	columns := make([]string, 0, len(l.tables[name].columnsByName))
	for column := range l.tables[name].columnsByName {
		// check if column is empty
		if len(column) > 0 {
			columns = append(columns, column)
		}
	}
	return columns
}

func (l *Loader) GetAvailableTablesInSchema() []string {
	tables := make([]string, len(l.tables))
	i := 0
	for table := range l.tables {
		tables[i] = table
		i++
	}
	return tables
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

// Setup creates the schemaName, cursors and history table where the <schemaBytes> is a byte array
// taken from somewhere.
func (l *Loader) Setup(ctx context.Context, schemaName string, userSql string, withPostgraphile bool) error {
	if userSql != "" {
		if err := l.dialect.ExecuteSetupScript(ctx, l, userSql); err != nil {
			return fmt.Errorf("exec userSql: %w", err)
		}
	}

	if err := l.setupCursorTable(ctx, schemaName, withPostgraphile); err != nil {
		return fmt.Errorf("setup cursor table: %w", err)
	}

	if err := l.setupHistoryTable(ctx, schemaName, withPostgraphile); err != nil {
		return fmt.Errorf("setup history table: %w", err)
	}

	return nil
}

func (l *Loader) setupCursorTable(ctx context.Context, schemaName string, withPostgraphile bool) error {
	query := l.dialect.GetCreateCursorQuery(schemaName, withPostgraphile)
	_, err := l.ExecContext(ctx, query)
	return err
}

func (l *Loader) setupHistoryTable(ctx context.Context, schemaName string, withPostgraphile bool) error {
	if l.dialect.OnlyInserts() {
		return nil
	}
	query := l.dialect.GetCreateHistoryQuery(schemaName, withPostgraphile)
	_, err := l.ExecContext(ctx, query)
	return err
}

// GetIdentifier returns <database>/<schema> suitable for user presentation
func (l *Loader) GetIdentifier() string {
	return fmt.Sprintf("%s/%s", l.dsn.schema, l.dsn.schema)
}

// GetIdentifier returns <database>/<schema> suitable for user presentation
func (l *Loader) GetDSN() *DSN {
	return l.dsn
}

type obfuscatedString string

func (s obfuscatedString) String() string {
	if len(s) == 0 {
		return "<unset>"
	}

	return "********"
}
