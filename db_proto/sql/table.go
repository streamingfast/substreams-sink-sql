package sql

import (
	"fmt"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/proto"
)

type PrimaryKey struct {
	Name      string
	DataType  DataType
	Index     int
	Generated bool
}

type ChildOf struct {
	ParentTable      string
	ParentTableField string
}

func NewChildOf(childOf string) (*ChildOf, error) {
	parts := strings.Split(childOf, " on ")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid child of format %q. expecting 'table_name on field_name' format", childOf)
	}

	return &ChildOf{
		ParentTable:      strings.TrimSpace(parts[0]),
		ParentTableField: strings.TrimSpace(parts[1]),
	}, nil
}

type Table struct {
	Name       string
	PrimaryKey *PrimaryKey
	ChildOf    *ChildOf
	Columns    []*Column
}

func NewTable(descriptor *desc.MessageDescriptor) (*Table, error) {
	tableInfo := proto.TableInfo(descriptor)
	if tableInfo == nil {
		return nil, nil
	}

	table := &Table{
		Name: descriptor.GetName(),
	}
	table.Name = tableInfo.Name

	if tableInfo.ChildOf != nil {
		co, err := NewChildOf(*tableInfo.ChildOf)
		if err != nil {
			return nil, fmt.Errorf("error parsing child of: %w", err)
		}
		table.ChildOf = co
	}

	err := table.processColumns(descriptor)
	if err != nil {
		return nil, fmt.Errorf("error processing fields for table %q: %w", descriptor.GetName(), err)
	}

	return table, nil
}

func (t *Table) processColumns(descriptor *desc.MessageDescriptor) error {
	for idx, fieldDescriptor := range descriptor.GetFields() {
		column, err := NewColumn(fieldDescriptor)
		if err != nil {
			return fmt.Errorf("error processing column %q: %w", fieldDescriptor.GetName(), err)
		}
		if column.IsPrimaryKey {
			if t.PrimaryKey != nil {
				return fmt.Errorf("multiple primary keys are not supported in message")
			}

			t.PrimaryKey = &PrimaryKey{
				Name:     column.Name,
				DataType: column.DataType,
				Index:    idx,
			}
		}
		t.Columns = append(t.Columns, column)
	}

	if t.PrimaryKey == nil {
		t.PrimaryKey = &PrimaryKey{
			Name:      "id",
			DataType:  TypeInteger,
			Generated: true,
		}
	}

	return nil
}

func (t *Table) FullName(schema *Schema) string {
	return TableName(schema, t.Name)
}

func TableName(schema *Schema, tableName string) string {
	return fmt.Sprintf("%s.%s", schema.Name, tableName)
}
