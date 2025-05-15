package clickhouse

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DataType string

const (
	TypeInteger8   DataType = "Int8"
	TypeInteger16  DataType = "Int16"
	TypeInteger32  DataType = "Int32"
	TypeInteger64  DataType = "Int64"
	TypeInteger128 DataType = "Int128"
	TypeInteger256 DataType = "Int256"

	TypeUInt8   DataType = "UInt8"
	TypeUInt16  DataType = "UInt16"
	TypeUInt32  DataType = "UInt32"
	TypeUInt64  DataType = "UInt64"
	TypeUInt128 DataType = "UInt128"
	TypeUInt256 DataType = "UInt256"

	TypeFloat32 DataType = "Float32"
	TypeFloat64 DataType = "Float64"

	TypeBool    DataType = "Bool"
	TypeVarchar DataType = "VARCHAR"

	TypeDateTime DataType = "DateTime"
)

func (s DataType) String() string {
	return string(s)
}

func MapFieldType(fd *desc.FieldDescriptor) DataType {
	t := fd.GetType()
	switch t {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		switch fd.GetMessageType().GetFullyQualifiedName() {
		case "google.protobuf.Timestamp":
			return TypeDateTime
		default:
			panic(fmt.Sprintf("Message type not supported: %s", fd.GetMessageType().GetFullyQualifiedName()))
		}
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		return TypeInteger32
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return TypeBool
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return TypeInteger32
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return TypeInteger64
	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		return TypeUInt64
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return TypeUInt32
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return TypeFloat32
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return TypeFloat64
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return TypeVarchar
	default:
		panic(fmt.Sprintf("unsupported type: %s", t))
	}
}

func ValueToString(value any) (s string) {
	switch v := value.(type) {
	case string:
		s = "'" + strings.ReplaceAll(strings.ReplaceAll(v, "'", "''"), "\\", "\\\\") + "'"
	case int64:
		s = strconv.FormatInt(v, 10)
	case int32:
		s = strconv.FormatInt(int64(v), 10)
	case int:
		s = strconv.FormatInt(int64(v), 10)
	case uint64:
		s = strconv.FormatUint(v, 10)
	case uint32:
		s = strconv.FormatUint(uint64(v), 10)
	case uint:
		s = strconv.FormatUint(uint64(v), 10)
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case []uint8:
		s = "'" + base64.StdEncoding.EncodeToString(v) + "'"
	case bool:
		s = strconv.FormatBool(v)
	case time.Time:
		s = "'" + v.Format(time.DateTime) + "'"
	case *timestamppb.Timestamp:
		s = "'" + v.AsTime().Format(time.DateTime) + "'"
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
	return
}
