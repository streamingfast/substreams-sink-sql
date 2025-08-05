package risingwave

import (
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
	// Numeric types - aligning with RisingWave documentation
	TypeSmallInt DataType = "SMALLINT"         // Two-byte integer
	TypeInteger  DataType = "INTEGER"          // Four-byte integer
	TypeBigInt   DataType = "BIGINT"           // Eight-byte integer
	TypeNumeric  DataType = "NUMERIC"          // Exact numeric (28 decimal digits precision)
	TypeReal     DataType = "REAL"             // Single precision floating-point (4 bytes)
	TypeDouble   DataType = "DOUBLE PRECISION" // Double precision floating-point (8 bytes)

	// Boolean type
	TypeBool DataType = "BOOLEAN" // Logical Boolean (true, false, or null)

	// String types
	TypeVarchar DataType = "VARCHAR" // Variable-length character string (no length limit specified)
	TypeText    DataType = "VARCHAR" // Use VARCHAR instead of TEXT for RisingWave compatibility

	// Binary type
	TypeBytea DataType = "BYTEA" // Binary strings (hex format)

	// Date and time types
	TypeDate        DataType = "DATE"                     // Calendar date (year, month, day)
	TypeTime        DataType = "TIME"                     // Time of day (no time zone)
	TypeTimestamp   DataType = "TIMESTAMP"                // Date and time (no time zone)
	TypeTimestamptz DataType = "TIMESTAMP WITH TIME ZONE" // Timestamp with time zone
	TypeInterval    DataType = "INTERVAL"                 // Time span

	// Complex types - basic definitions
	TypeStruct DataType = "STRUCT" // Nested data structure
	TypeArray  DataType = "ARRAY"  // Ordered list of elements
	TypeMap    DataType = "MAP"    // Key-value pairs
	TypeJsonb  DataType = "JSONB"  // Binary JSON value
)

func (s DataType) String() string {
	return string(s)
}

func IsWellKnownType(fd *desc.FieldDescriptor) bool {
	switch fd.GetMessageType().GetFullyQualifiedName() {
	case "google.protobuf.Timestamp":
		return true
	default:
		return false
	}
}

func MapFieldType(fd *desc.FieldDescriptor) DataType {
	t := fd.GetType()
	switch t {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		switch fd.GetMessageType().GetFullyQualifiedName() {
		case "google.protobuf.Timestamp":
			return TypeTimestamptz // Use timestamptz for protobuf timestamps
		default:
			panic(fmt.Sprintf("Message type not supported: %s", fd.GetMessageType().GetFullyQualifiedName()))
		}
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return TypeBool
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return TypeInteger
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return TypeBigInt
	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		return TypeNumeric // Use NUMERIC for large unsigned integers
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return TypeBigInt // Use BIGINT for 32-bit unsigned (to avoid overflow)
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return TypeReal // Use REAL for single precision
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return TypeDouble
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return TypeBytea // Use BYTEA for binary data
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		return TypeVarchar // Store enums as varchar
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
		// For large unsigned integers, use numeric literal
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
		// RisingWave expects hex format for bytea: '\x...'
		s = "'\\x" + strings.ToUpper(fmt.Sprintf("%x", v)) + "'"
	case bool:
		s = strconv.FormatBool(v)
	case time.Time:
		// Use RFC3339 format for timestamps
		s = "'" + v.Format(time.RFC3339) + "'"
	case *timestamppb.Timestamp:
		// Convert protobuf timestamp to timestamptz format
		s = "'" + v.AsTime().Format(time.RFC3339) + "'"
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
	return
}
