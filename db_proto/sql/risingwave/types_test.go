package risingwave

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValueToString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		// String values
		{"simple string", "hello", "'hello'"},
		{"string with quotes", "hello'world", "'hello''world'"},
		{"string with backslash", "hello\\world", "'hello\\\\world'"},
		{"empty string", "", "''"},

		// Integer values
		{"int64", int64(123), "123"},
		{"int64 negative", int64(-456), "-456"},
		{"int32", int32(456), "456"},
		{"int", int(789), "789"},

		// Unsigned integer values
		{"uint64", uint64(123), "123"},
		{"uint32", uint32(456), "456"},
		{"uint", uint(789), "789"},

		// Float values
		{"float64", float64(123.45), "123.45"},
		{"float32", float32(67.89), "67.89"},

		// Boolean values
		{"bool true", true, "true"},
		{"bool false", false, "false"},

		// Byte slice (should be hex encoded with uppercase)
		{"bytes", []uint8{0xDE, 0xAD, 0xBE, 0xEF}, "'\\xDEADBEEF'"},
		{"empty bytes", []uint8{}, "'\\x'"},

		// Time values
		{"time", time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC), "'2023-01-15T10:30:00Z'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValueToString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToStringTimestamp(t *testing.T) {
	// Test protobuf timestamp
	testTime := time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)
	pbTime := timestamppb.New(testTime)
	result := ValueToString(pbTime)
	assert.Equal(t, "'2023-01-15T10:30:00Z'", result)
}

func TestValueToStringPanic(t *testing.T) {
	// Test unsupported type should panic
	assert.Panics(t, func() {
		ValueToString(complex64(1 + 2i))
	})
}

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		dataType DataType
		expected string
	}{
		{TypeSmallInt, "SMALLINT"},
		{TypeInteger, "INTEGER"},
		{TypeBigInt, "BIGINT"},
		{TypeNumeric, "NUMERIC"},
		{TypeReal, "REAL"},
		{TypeDouble, "DOUBLE PRECISION"},
		{TypeBool, "BOOLEAN"},
		{TypeVarchar, "VARCHAR"},
		{TypeText, "VARCHAR"},
		{TypeBytea, "BYTEA"},
		{TypeDate, "DATE"},
		{TypeTime, "TIME"},
		{TypeTimestamp, "TIMESTAMP"},
		{TypeTimestamptz, "TIMESTAMP WITH TIME ZONE"},
		{TypeInterval, "INTERVAL"},
		{TypeStruct, "STRUCT"},
		{TypeArray, "ARRAY"},
		{TypeMap, "MAP"},
		{TypeJsonb, "JSONB"},
	}

	for _, tt := range tests {
		t.Run(string(tt.dataType), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dataType.String())
		})
	}
}
