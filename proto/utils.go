package proto

import (
	"errors"
	"fmt"

	proto "github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TableInfo(d *desc.MessageDescriptor) *schema.Table {
	msgOptions := d.GetOptions().(*descriptorpb.MessageOptions)

	ext, err := proto.GetExtension(msgOptions, schema.E_Table)

	if errors.Is(err, proto.ErrMissingExtension) {
		return nil
	} else if err != nil {
		return nil
	} else {
		table, ok := ext.(*schema.Table)
		if ok {
			if table.Name == "" {
				panic(fmt.Sprintf("table name is required for message %q", d.GetName()))
			}
			return table
		} else {
			return nil
		}
	}
}

func FieldInfo(d *desc.FieldDescriptor) *schema.Column {
	options := d.GetOptions().(*descriptorpb.FieldOptions)

	ext, err := proto.GetExtension(options, schema.E_Field)

	if errors.Is(err, proto.ErrMissingExtension) {
		return nil
	} else if err != nil {
		return nil
	} else {
		f, ok := ext.(*schema.Column)
		if ok {
			return f
		} else {
			return nil
		}
	}
}
