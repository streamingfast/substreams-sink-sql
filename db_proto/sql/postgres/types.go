package postgres

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
	TypeNumeric   DataType = "NUMERIC"
	TypeInteger   DataType = "INTEGER"
	TypeBool      DataType = "BOOLEAN"
	TypeBigInt    DataType = "BIGINT"
	TypeDecimal   DataType = "DECIMAL"
	TypeDouble    DataType = "DOUBLE PRECISION"
	TypeText      DataType = "TEXT"
	TypeBlob      DataType = "BLOB"
	TypeVarchar   DataType = "VARCHAR(255)"
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

func MapFieldType(fd *desc.FieldDescriptor) DataType {
	t := fd.GetType()
	switch t {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		switch fd.GetMessageType().GetFullyQualifiedName() {
		case "google.protobuf.Timestamp":
			return TypeTimestamp
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
		return TypeNumeric
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return TypeNumeric
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return TypeDecimal
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return TypeDouble
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return TypeText
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		return TypeText
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
		s = "'" + strconv.FormatUint(v, 10) + "'"
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
		s = "'" + v.Format(time.RFC3339) + "'"
	case *timestamppb.Timestamp:
		s = "'" + v.AsTime().Format(time.RFC3339) + "'"
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
	return
}
