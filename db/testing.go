package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

func NewTestLoader(
	zlog *zap.Logger,
	tracer logging.Tracer,
	schema string,
	tables map[string]*TableInfo,
) (*Loader, *TestTx) {

	loader, err := NewLoader("psql://x:5432/x", 0, OnModuleHashMismatchIgnore, zlog, tracer)
	if err != nil {
		panic(err)
	}
	loader.testTx = &TestTx{}
	loader.tables = tables
	loader.schema = schema
	loader.cursorTable = tables["cursors"]
	return loader, loader.testTx

}

func TestTables(schema string) map[string]*TableInfo {
	return map[string]*TableInfo{
		"xfer": mustNewTableInfo(schema, "xfer", []string{"id"}, map[string]*ColumnInfo{
			"id":   NewColumnInfo("id", "text", ""),
			"from": NewColumnInfo("from", "text", ""),
			"to":   NewColumnInfo("to", "text", ""),
		}),
		"cursors": mustNewTableInfo(schema, "cursors", []string{"id"}, map[string]*ColumnInfo{
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

func (t *TestTx) AppendResp(in *sql.Rows) {
	t.next = append(t.next, in)

}

func (t *TestTx) QueryContext(ctx context.Context, query string, args ...any) (out *sql.Rows, err error) {
	if len(t.next) == 0 {
		return nil, fmt.Errorf("testTx queried but no responses were set")
	}

	out, t.next = t.next[0], t.next[1:]
	return out, nil
}

type testResult struct{}

func (t *testResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (t *testResult) RowsAffected() (int64, error) {
	return 1, nil
}
