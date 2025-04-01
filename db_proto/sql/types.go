package sql

import (
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
)

type DataType string

const (
	TypeInteger DataType = "INTEGER"
	TypeBool    DataType = "BOOLEAN"
	TypeBigInt  DataType = "BIGINT"
	TypeDecimal DataType = "DECIMAL"
	TypeDouble  DataType = "DOUBLE PRECISION"
	TypeText    DataType = "TEXT"
	TypeBlob    DataType = "BLOB"
	TypeVarchar DataType = "VARCHAR(255)"
)

func (s DataType) String() string {
	return string(s)
}

func mapFieldType(field *desc.FieldDescriptor) DataType {

	switch field.GetType() {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		return TypeInteger
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return TypeBool
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return TypeInteger
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return TypeBigInt
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return TypeDecimal
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return TypeDouble
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return TypeBlob
	default:
		return TypeText
	}
}
