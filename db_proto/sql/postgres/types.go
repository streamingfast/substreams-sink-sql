package postgres

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/bytes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DataType string

const (
	TypeNumeric   DataType = "NUMERIC"
	TypeInteger   DataType = "INTEGER"
	TypeBool      DataType = "BOOLEAN"
	TypeBigInt    DataType = "BIGINT"
	TypeDecimal   DataType = "DECIMAL"
	TypeDouble    DataType = "DOUBLE PRECISION"
	TypeText      DataType = "TEXT"
	TypeBlob      DataType = "BLOB"
	TypeVarchar   DataType = "VARCHAR(255)"
	TypeBytea     DataType = "BYTEA"
	TypeTimestamp DataType = "TIMESTAMP"
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

func MapFieldType(fd *desc.FieldDescriptor, bytesEncoding bytes.Encoding) DataType {
	t := fd.GetType()
	var baseType DataType

	switch t {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		switch fd.GetMessageType().GetFullyQualifiedName() {
		case "google.protobuf.Timestamp":
			baseType = TypeTimestamp
		default:
			panic(fmt.Sprintf("Message type not supported: %s", fd.GetMessageType().GetFullyQualifiedName()))
		}
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		baseType = TypeBool
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		baseType = TypeInteger
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		baseType = TypeBigInt
	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		baseType = TypeNumeric
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		baseType = TypeNumeric
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		baseType = TypeDecimal
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		baseType = TypeDouble
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		baseType = TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		if bytesEncoding.IsStringType() {
			baseType = TypeText
		} else {
			baseType = TypeBytea
		}
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		baseType = TypeText
	default:
		panic(fmt.Sprintf("unsupported type: %s", t))
	}

	// If field is repeated, wrap the base type as an array
	if fd.IsRepeated() {
		return DataType(fmt.Sprintf("%s[]", baseType))
	}

	return baseType
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
			// For raw encoding, use PostgreSQL bytea format
			s = "'" + base64.StdEncoding.EncodeToString(v) + "'"
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
		s = "'" + v.Format(time.RFC3339) + "'"
	case *timestamppb.Timestamp:
		s = "'" + v.AsTime().Format(time.RFC3339) + "'"
	// Handle array types for PostgreSQL
	case []interface{}:
		var elements []string
		for _, elem := range v {
			elements = append(elements, ValueToString(elem, bytesEncoding))
		}
		s = "array[" + strings.Join(elements, ",") + "]"
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
	return
}
