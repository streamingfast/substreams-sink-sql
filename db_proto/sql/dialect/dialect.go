package dialect

import (
	"database/sql"

	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

type Dialect interface {
	Hash() string
	FullTableName(table *schema.Table) string
	CreateDatabase(tx *sql.Tx) error
	ApplyConstraints(tx *sql.Tx) error
	GetCursorSql() string
	GetTable(table string) *schema.Table
	GetInsert(table string) string
	GetInserts() map[string]string
}

type BaseDialect struct {
	createTableSql      map[string]string
	primaryKeySql       []*Constraint
	foreignKeySql       []*Constraint
	uniqueConstraintSql []*Constraint
	insertSql           map[string]string
	tableRegistry       map[string]*schema.Table
	logger              *zap.Logger
}

func (d *BaseDialect) AddInsertSql(table string, sql string) {
	d.insertSql[table] = sql
}

func (d *BaseDialect) GetInsertSql(table string) string {
	return d.insertSql[table]
}

func (d *BaseDialect) GetAllInsertSql() []string {
	return maps.Values(d.insertSql)
}

func (d *BaseDialect) AddCreateTableSql(table string, sql string) {
	d.createTableSql[table] = sql
}

func (d *BaseDialect) GetCreateTableSql(table string) string {
	return d.createTableSql[table]
}

func (d *BaseDialect) AddPrimaryKeySql(table string, sql string) {
	d.primaryKeySql = append(d.primaryKeySql, &Constraint{table: table, sql: sql})
}

func (d *BaseDialect) AddForeignKeySql(table string, sql string) {
	d.foreignKeySql = append(d.foreignKeySql, &Constraint{table: table, sql: sql})
}

func (d *BaseDialect) AddUniqueConstraintSql(table string, sql string) {
	d.uniqueConstraintSql = append(d.uniqueConstraintSql, &Constraint{table: table, sql: sql})
}

func (d *BaseDialect) GetTable(table string) *schema.Table {
	return d.tableRegistry[table]
}
