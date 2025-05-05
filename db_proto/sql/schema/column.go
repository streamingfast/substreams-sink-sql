package schema

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/proto"
)

type Column struct {
	Name            string
	ForeignKey      *ForeignKey
	FieldDescriptor *desc.FieldDescriptor
	IsPrimaryKey    bool
	IsUnique        bool
	IsRepeated      bool
	IsExtension     bool
	//todo: naming ...
	IsMessage bool
	Message   string
}

func NewColumn(d *desc.FieldDescriptor) (*Column, error) {
	out := &Column{
		Name:            d.GetName(),
		FieldDescriptor: d,
		IsRepeated:      d.IsRepeated(),
		IsMessage:       d.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE,
		IsExtension:     d.IsExtension(),
	}

	fieldInfo := proto.FieldInfo(d)
	if fieldInfo != nil {
		if fieldInfo.Name != nil {
			out.Name = *fieldInfo.Name
		}
		if fieldInfo.ForeignKey != nil {
			fk, err := NewForeignKey(*fieldInfo.ForeignKey)
			if err != nil {
				return nil, fmt.Errorf("error parsing foreign key %s: %w", *fieldInfo.ForeignKey, err)
			}
			out.ForeignKey = fk
		}
		out.IsPrimaryKey = fieldInfo.PrimaryKey
		out.IsUnique = fieldInfo.Unique
	}

	if out.IsMessage {
		out.Message = d.GetMessageType().GetName()
	}
	return out, nil
}

func (c *Column) QuotedName() string {
	return fmt.Sprintf("%q", c.Name)
}

type ForeignKey struct {
	Table      string
	TableField string
}

func NewForeignKey(foreignKey string) (*ForeignKey, error) {
	parts := strings.Split(foreignKey, " on ")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid foreign key format %q. expecting 'table_name on field_name' format", foreignKey)
	}
	return &ForeignKey{
		Table:      strings.TrimSpace(parts[0]),
		TableField: strings.TrimSpace(parts[1]),
	}, nil
}
