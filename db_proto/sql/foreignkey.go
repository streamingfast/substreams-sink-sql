package sql

import "fmt"

type foreignKey struct {
	name         string
	table        string
	field        string
	foreignTable string
	foreignField string
}

type Constraint struct {
	table string
	sql   string
}

func (f *foreignKey) String() string {
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s  FOREIGN KEY (%s) REFERENCES %s(%s)", f.table, f.name, f.field, f.foreignTable, f.foreignField)
}
