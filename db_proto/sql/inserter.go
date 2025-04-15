package sql

import "database/sql"

type Inserter interface {
	Insert(table string, values []any, txWrapper func(stmt *sql.Stmt) *sql.Stmt) error
	Flush(tx *sql.Tx) error
}
