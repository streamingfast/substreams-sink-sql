package sql

import (
	"database/sql"

	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

type Dialect interface {
	SchemaHash() string
	FullTableName(table *schema.Table) string
	CreateDatabase(tx *sql.Tx) error
	ApplyConstraints(tx *sql.Tx) error
	GetTable(table string) *schema.Table
	GetTables() []*schema.Table
}

type BaseDialect struct {
	CreateTableSql      map[string]string
	PrimaryKeySql       []*Constraint
	ForeignKeySql       []*Constraint
	UniqueConstraintSql []*Constraint
	TableRegistry       map[string]*schema.Table
	Logger              *zap.Logger
}

func NewBaseDialect(registry map[string]*schema.Table, logger *zap.Logger) *BaseDialect {
	return &BaseDialect{
		CreateTableSql: make(map[string]string),
		TableRegistry:  registry,
		Logger:         logger,
	}
}

func (d *BaseDialect) AddCreateTableSql(table string, sql string) {
	d.CreateTableSql[table] = sql
}

func (d *BaseDialect) GetCreateTableSql(table string) string {
	return d.CreateTableSql[table]
}

func (d *BaseDialect) AddPrimaryKeySql(table string, sql string) {
	d.PrimaryKeySql = append(d.PrimaryKeySql, &Constraint{Table: table, Sql: sql})
}

func (d *BaseDialect) AddForeignKeySql(table string, sql string) {
	d.ForeignKeySql = append(d.ForeignKeySql, &Constraint{Table: table, Sql: sql})
}

func (d *BaseDialect) AddUniqueConstraintSql(table string, sql string) {
	d.UniqueConstraintSql = append(d.UniqueConstraintSql, &Constraint{Table: table, Sql: sql})
}

func (d *BaseDialect) GetTable(table string) *schema.Table {
	return d.TableRegistry[table]
}

func (d *BaseDialect) GetTables() []*schema.Table {
	return maps.Values(d.TableRegistry)
}
