package clickhouse

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/bytes"
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

func MapFieldType(fd *desc.FieldDescriptor, bytesEncoding bytes.Encoding) DataType {
	t := fd.GetType()
	var baseType DataType

	switch t {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		switch fd.GetMessageType().GetFullyQualifiedName() {
		case "google.protobuf.Timestamp":
			baseType = TypeDateTime
		default:
			panic(fmt.Sprintf("Message type not supported: %s", fd.GetMessageType().GetFullyQualifiedName()))
		}
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		baseType = TypeInteger32
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		baseType = TypeBool
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		baseType = TypeInteger32
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		baseType = TypeInteger64
	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		baseType = TypeUInt64
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		baseType = TypeUInt32
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		baseType = TypeFloat32
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		baseType = TypeFloat64
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		baseType = TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		baseType = TypeVarchar
	default:
		panic(fmt.Sprintf("unsupported type: %s", t))
	}

	// If field is repeated, wrap the base type as an array
	if fd.IsRepeated() {
		return DataType(fmt.Sprintf("Array(%s)", baseType))
	}

	if fd.IsProto3Optional() {
		return DataType(fmt.Sprintf("Nullable(%s)", baseType))
	}

	return baseType
}

func ColInputForColumn(fd *desc.FieldDescriptor, bytesEncoding bytes.Encoding) proto.ColInput {
	var baseInput proto.ColInput

	switch fd.GetType() {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		switch fd.GetMessageType().GetFullyQualifiedName() {
		case "google.protobuf.Timestamp":
			baseInput = &proto.ColDateTime{}
		default:
			panic(fmt.Sprintf("Message type not supported: %s", fd.GetMessageType().GetFullyQualifiedName()))
		}
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		baseInput = &proto.ColInt32{}
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		baseInput = &proto.ColBool{}
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		baseInput = &proto.ColInt32{}
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		baseInput = &proto.ColInt64{}
	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		baseInput = &proto.ColUInt64{}
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		baseInput = &proto.ColUInt32{}
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		baseInput = &proto.ColFloat32{}
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		baseInput = &proto.ColFloat64{}
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		baseInput = &proto.ColStr{}
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		if bytesEncoding.IsStringType() {
			baseInput = &proto.ColStr{}
		} else {
			baseInput = &proto.ColBytes{}
		}
	default:
		panic(fmt.Sprintf("unsupported type: %s", fd.GetType()))
	}

	// If field is repeated, wrap the base input as an array
	if fd.IsRepeated() {
		switch base := baseInput.(type) {
		case *proto.ColInt32:
			return proto.NewArray(base)
		case *proto.ColInt64:
			return proto.NewArray(base)
		case *proto.ColUInt32:
			return proto.NewArray(base)
		case *proto.ColUInt64:
			return proto.NewArray(base)
		case *proto.ColFloat32:
			return proto.NewArray(base)
		case *proto.ColFloat64:
			return proto.NewArray(base)
		case *proto.ColBool:
			return proto.NewArray(base)
		case *proto.ColStr:
			return proto.NewArray(base)
		case *proto.ColBytes:
			return proto.NewArray(base)
		case *proto.ColDateTime:
			return proto.NewArray(base)
		default:
			panic(fmt.Sprintf("unsupported array base type: %T", base))
		}
	}

	return baseInput
}

func ValueToString(value any, bytesEncoding bytes.Encoding) (s string) {
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
		if bytesEncoding == bytes.EncodingRaw {
			// For raw encoding, return as hex string for SQL
			s = "unhex('" + fmt.Sprintf("%x", v) + "')"
		} else {
			encoded, err := bytesEncoding.EncodeBytes(v)
			if err != nil {
				panic(fmt.Sprintf("failed to encode bytes: %v", err))
			}
			s = "'" + encoded.(string) + "'"
		}
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
