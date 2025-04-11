package sql

type Dialect interface {
	Init(schema *Schema) error
	HandleTable(schema *Schema, table *Table) error
}
