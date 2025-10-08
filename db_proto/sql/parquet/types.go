package parquet

import (
	"fmt"

	"github.com/streamingfast/substreams-sink-sql/bytes"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	v1 "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ParquetType represents a parquet column type
type ParquetType string

const (
	TypeBoolean   ParquetType = "BOOLEAN"
	TypeInt32     ParquetType = "INT32"
	TypeInt64     ParquetType = "INT64"
	TypeFloat     ParquetType = "FLOAT"
	TypeDouble    ParquetType = "DOUBLE"
	TypeString    ParquetType = "STRING"
	TypeBinary    ParquetType = "BINARY"
	TypeTimestamp ParquetType = "TIMESTAMP"
	TypeDecimal   ParquetType = "DECIMAL"
)

func (t ParquetType) String() string {
	return string(t)
}

// MapFieldType maps a protobuf field descriptor to a parquet type
func MapFieldType(fd protoreflect.FieldDescriptor, bytesEncoding bytes.Encoding, column *schema.Column) ParquetType {
	kind := fd.Kind()
	var baseType ParquetType

	switch kind {
	case protoreflect.MessageKind:
		switch string(fd.Message().FullName()) {
		case "google.protobuf.Timestamp":
			baseType = TypeTimestamp
		default:
			panic(fmt.Sprintf("Message type not supported: %s", string(fd.Message().FullName())))
		}
	case protoreflect.EnumKind:
		baseType = TypeString // Enums stored as strings in parquet
	case protoreflect.BoolKind:
		baseType = TypeBoolean
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		baseType = TypeInt32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		baseType = TypeInt64
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		baseType = TypeInt64 // Parquet doesn't have unsigned types, use signed
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		baseType = TypeInt32 // Parquet doesn't have unsigned types, use signed
	case protoreflect.FloatKind:
		baseType = TypeFloat
	case protoreflect.DoubleKind:
		baseType = TypeDouble
	case protoreflect.StringKind:
		if column.ConvertTo != nil && column.ConvertTo.Convertion != nil {
			switch column.ConvertTo.Convertion.(type) {
			case *v1.StringConvertion_Int128:
				baseType = TypeDecimal // Use decimal for large integers
			case *v1.StringConvertion_Uint128:
				baseType = TypeDecimal
			case *v1.StringConvertion_Int256:
				baseType = TypeDecimal
			case *v1.StringConvertion_Uint256:
				baseType = TypeDecimal
			case *v1.StringConvertion_Decimal128:
				baseType = TypeDecimal
			case *v1.StringConvertion_Decimal256:
				baseType = TypeDecimal
			default:
				baseType = TypeString
			}
		} else {
			baseType = TypeString
		}
	case protoreflect.BytesKind:
		baseType = TypeBinary
	default:
		panic(fmt.Sprintf("unsupported type: %s", kind))
	}

	// If field is repeated, wrap the base type as an array
	if fd.IsList() {
		// In parquet, arrays are represented as repeated groups
		// We'll handle this in the schema generation
		return baseType
	}

	return baseType
}

// GetParquetSchemaType returns the full parquet type including array information
func GetParquetSchemaType(fd protoreflect.FieldDescriptor, bytesEncoding bytes.Encoding, column *schema.Column) string {
	baseType := MapFieldType(fd, bytesEncoding, column)

	if fd.IsList() {
		return fmt.Sprintf("ARRAY<%s>", baseType)
	}

	// Handle decimal types with precision/scale
	if column.ConvertTo != nil && column.ConvertTo.Convertion != nil {
		switch conv := column.ConvertTo.Convertion.(type) {
		case *v1.StringConvertion_Decimal128:
			return fmt.Sprintf("DECIMAL(38,%d)", conv.Decimal128.Scale)
		case *v1.StringConvertion_Decimal256:
			return fmt.Sprintf("DECIMAL(76,%d)", conv.Decimal256.Scale)
		}
	}

	return string(baseType)
}
