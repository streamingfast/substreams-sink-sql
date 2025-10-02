package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/bytes"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	v1 "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
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

	TypeDecimal128 = "Decimal128"
	TypeDecimal256 = "Decimal256"

	TypeBool    DataType = "Bool"
	TypeVarchar DataType = "VARCHAR"

	TypeDateTime DataType = "DateTime"
)

func (s DataType) String() string {
	return string(s)
}

func MapFieldType(fd *desc.FieldDescriptor, bytesEncoding bytes.Encoding, column *schema.Column) DataType {
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
		if column.ConvertTo != nil && column.ConvertTo.Convertion != nil {
			switch column.ConvertTo.Convertion.(type) {
			case *v1.StringConvertion_Int128:
				baseType = TypeInteger128
			case *v1.StringConvertion_Uint128:
				baseType = TypeUInt128
			case *v1.StringConvertion_Int256:
				baseType = TypeInteger256
			case *v1.StringConvertion_Uint256:
				baseType = TypeUInt256
			case *v1.StringConvertion_Decimal128:
				decimal128Conv := column.ConvertTo.Convertion.(*v1.StringConvertion_Decimal128)
				baseType = DataType(fmt.Sprintf("Decimal128(%d)", decimal128Conv.Decimal128.Scale))
			case *v1.StringConvertion_Decimal256:
				decimal256Conv := column.ConvertTo.Convertion.(*v1.StringConvertion_Decimal256)
				baseType = DataType(fmt.Sprintf("Decimal256(%d)", decimal256Conv.Decimal256.Scale))
			default:
				panic(fmt.Sprintf("unsupported type: %s", t))
			}
		} else {
			baseType = TypeVarchar
		}

	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		baseType = TypeVarchar
	default:
		panic(fmt.Sprintf("unsupported type: %s", t))
	}

	// If field is repeated, wrap the base type as an array
	if fd.IsRepeated() {
		return DataType(fmt.Sprintf("Array(%s)", baseType))
	}

	//if fd.IsProto3Optional() {
	//	return DataType(fmt.Sprintf("Nullable(%s)", baseType))
	//}

	return baseType
}

func ColInputForColumn(fd *desc.FieldDescriptor, bytesEncoding bytes.Encoding, column *schema.Column) proto.ColInput {
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
		if column.ConvertTo != nil && column.ConvertTo.Convertion != nil {
			switch column.ConvertTo.Convertion.(type) {
			case *v1.StringConvertion_Int128:
				baseInput = &proto.ColInt128{}
			case *v1.StringConvertion_Uint128:
				baseInput = &proto.ColUInt128{}
			case *v1.StringConvertion_Int256:
				baseInput = &proto.ColInt256{}
			case *v1.StringConvertion_Uint256:
				baseInput = &proto.ColUInt256{}
			case *v1.StringConvertion_Decimal128:
				innerCol := &proto.ColDecimal128{}
				scale := (column.ConvertTo.Convertion.(*v1.StringConvertion_Decimal128)).Decimal128.Scale
				baseInput = &ColScaledDecimal128{
					ColDecimal128: innerCol,
					scale:         uint8(scale),
				}
			case *v1.StringConvertion_Decimal256:
				innerCol := &proto.ColDecimal256{}
				scale := (column.ConvertTo.Convertion.(*v1.StringConvertion_Decimal256)).Decimal256.Scale
				baseInput = &ColScaledDecimal256{
					ColDecimal256: innerCol,
					scale:         uint8(scale),
				}
			default:
				panic(fmt.Sprintf("unsupported type: %s", fd.GetType()))
			}
		} else {
			baseInput = &proto.ColStr{}
		}
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
