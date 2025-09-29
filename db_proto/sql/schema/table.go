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
	Name        string
	PrimaryKey  *PrimaryKey
	ChildOf     *ChildOf
	Columns     []*Column
	Ordinal     int
	PbTableInfo *pbSchmema.Table
}

func NewTable(descriptor *desc.MessageDescriptor, tableInfo *pbSchmema.Table, ordinal int) (*Table, error) {
	table := &Table{
		Name:        descriptor.GetName(),
		Ordinal:     ordinal,
		PbTableInfo: tableInfo,
	}
	table.Name = tableInfo.Name

	typeName := descriptor.GetName()
	isTimestamp := typeName == ".google.protobuf.Timestamp" || typeName == "Timestamp"
	if isTimestamp {
		return nil, nil
	}

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

	if len(table.Columns) == 0 {
		return nil, nil
	}

	return table, nil
}

func (t *Table) processColumns(descriptor *desc.MessageDescriptor) error {
	for idx, fieldDescriptor := range descriptor.GetFields() {

		if fieldDescriptor.GetOneOf() != nil {
			continue
		}

		if fieldDescriptor.IsRepeated() {
			if fieldDescriptor.GetType() == descriptor2.FieldDescriptorProto_TYPE_MESSAGE { //This will be handled by table relations
				continue
			}
			// Allow repeated scalar fields to be processed as array columns
		}

		if fieldDescriptor.GetType() == descriptor2.FieldDescriptorProto_TYPE_MESSAGE {
			typeName := fieldDescriptor.GetMessageType().GetName()
			isTimestamp := typeName == ".google.protobuf.Timestamp" || typeName == "Timestamp"
			if !isTimestamp {
				continue
			}
		}

		column, err := NewColumn(fieldDescriptor)
		if err != nil {
			return fmt.Errorf("error processing column %q: %w", fieldDescriptor.GetName(), err)
		}

		if column.IsPrimaryKey {
			if t.PrimaryKey != nil {
				return fmt.Errorf("multiple field mark has primary keys are not supported")
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
