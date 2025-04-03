package db

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/streamingfast/logging"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"
)

const testCursorTableName = "cursors"
const testHistoryTableName = "substreams_history"

func NewTestLoader(
	t *testing.T,
	zlog *zap.Logger,
	tracer logging.Tracer,
	schema string,
	tables map[string]*TableInfo,
) (*Loader, *TestTx) {
	dsn, err := ParseDSN(fmt.Sprintf("psql://x:5432/x?schemaName=%s", schema))

	require.NoError(t, err)

	loader, err := NewLoader(
		dsn,
		testCursorTableName,
		testHistoryTableName,
		"",
		0, 0, 0,
		OnModuleHashMismatchIgnore.String(),
		nil,
		zlog, tracer,
	)
	require.NoError(t, err)

	loader.testTx = &TestTx{}
	loader.tables = tables
	loader.cursorTable = tables[testCursorTableName]
	return loader, loader.testTx

}

func TestTables(schema string) map[string]*TableInfo {
	return map[string]*TableInfo{
		"xfer": mustNewTableInfo(schema, "xfer", []string{"id"}, map[string]*ColumnInfo{
			"id":   NewColumnInfo("id", "text", ""),
			"from": NewColumnInfo("from", "text", ""),
			"to":   NewColumnInfo("to", "text", ""),
		}),
		testCursorTableName: mustNewTableInfo(schema, testCursorTableName, []string{"id"}, map[string]*ColumnInfo{
			"block_num": NewColumnInfo("id", "int64", ""),
			"block_id":  NewColumnInfo("from", "text", ""),
			"cursor":    NewColumnInfo("cursor", "text", ""),
			"id":        NewColumnInfo("id", "text", ""),
		}),
	}
}

func mustNewTableInfo(schema, name string, pkList []string, columnsByName map[string]*ColumnInfo) *TableInfo {
	ti, err := NewTableInfo(schema, name, pkList, columnsByName)
	if err != nil {
		panic(err)
	}
	return ti
}

type TestTx struct {
	queries []string
	next    []*sql.Rows
}

func (t *TestTx) Rollback() error {
	t.queries = append(t.queries, "ROLLBACK")
	return nil
}

func (t *TestTx) Commit() error {
	t.queries = append(t.queries, "COMMIT")
	return nil
}

func (t *TestTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	t.queries = append(t.queries, query)
	return &testResult{}, nil
}

func (t *TestTx) Results() []string {
	return t.queries
}

func (t *TestTx) QueryContext(ctx context.Context, query string, args ...any) (out *sql.Rows, err error) {
	t.queries = append(t.queries, query)
	return nil, nil
}

type testResult struct{}

func (t *testResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (t *testResult) RowsAffected() (int64, error) {
	return 1, nil
}
