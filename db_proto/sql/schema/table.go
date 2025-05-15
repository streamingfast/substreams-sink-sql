package schema

import (
	"fmt"
	"strings"

	descriptor2 "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	pbSchmema "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
)

type PrimaryKey struct {
	Name            string
	FieldDescriptor *desc.FieldDescriptor
	Index           int
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
	Ordinal    int
}

func NewTable(descriptor *desc.MessageDescriptor, tableInfo *pbSchmema.Table, ordinal int) (*Table, error) {
	table := &Table{
		Name:    descriptor.GetName(),
		Ordinal: ordinal,
	}
	table.Name = tableInfo.Name
	fmt.Println("new table name", table.Name)

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

		if fieldDescriptor.GetOneOf() != nil {
			continue
		}

		if fieldDescriptor.IsRepeated() {
			continue
		}

		if fieldDescriptor.GetType() == descriptor2.FieldDescriptorProto_TYPE_MESSAGE {
			if fieldDescriptor.AsFieldDescriptorProto().GetTypeName() != ".google.protobuf.Timestamp" {
				continue
			}
		}

		column, err := NewColumn(fieldDescriptor)
		if err != nil {
			return fmt.Errorf("error processing column %q: %w", fieldDescriptor.GetName(), err)
		}

		if column.IsPrimaryKey {
			if t.PrimaryKey != nil {
				return fmt.Errorf("multiple primary keys are not supported in message")
			}

			t.PrimaryKey = &PrimaryKey{
				Name:            column.Name,
				FieldDescriptor: fieldDescriptor,
				Index:           idx,
			}
		}
		t.Columns = append(t.Columns, column)
	}

	return nil
}
