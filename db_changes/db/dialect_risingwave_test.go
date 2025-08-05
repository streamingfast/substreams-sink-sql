package db

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRisingwavePrimaryKeyToJSON(t *testing.T) {
	tests := []struct {
		name   string
		keys   map[string]string
		expect string
	}{
		{
			name: "single key",
			keys: map[string]string{
				"id": "0xdeadbeef",
			},
			expect: `{"id":"0xdeadbeef"}`,
		},
		{
			name: "two keys",
			keys: map[string]string{
				"hash": "0xdeadbeef",
				"idx":  "5",
			},
			expect: `{"hash":"0xdeadbeef","idx":"5"}`,
		},
		{
			name: "determinism",
			keys: map[string]string{
				"bbb": "1",
				"ccc": "2",
				"aaa": "3",
				"ddd": "4",
			},
			expect: `{"aaa":"3","bbb":"1","ccc":"2","ddd":"4"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			jsonKey := primaryKeyToJSON(test.keys)
			assert.Equal(t, test.expect, jsonKey)
		})
	}
}

func TestRisingwaveJSONToPrimaryKey(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		expect map[string]string
	}{
		{
			name: "single key",
			in:   `{"id":"0xdeadbeef"}`,
			expect: map[string]string{
				"id": "0xdeadbeef",
			},
		},
		{
			name: "two keys",
			in:   `{"hash":"0xdeadbeef","idx":"5"}`,
			expect: map[string]string{
				"hash": "0xdeadbeef",
				"idx":  "5",
			},
		},
		{
			name: "determinism",
			in:   `{"aaa":"3","bbb":"1","ccc":"2","ddd":"4"}`,
			expect: map[string]string{
				"bbb": "1",
				"ccc": "2",
				"aaa": "3",
				"ddd": "4",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := jsonToPrimaryKey(test.in)
			require.NoError(t, err)
			assert.Equal(t, test.expect, out)
		})
	}
}

func TestRisingwaveGetPrimaryKeyFakeEmptyValues(t *testing.T) {
	tests := []struct {
		name       string
		primaryKey map[string]string
		expected   string
	}{
		{
			name: "single key",
			primaryKey: map[string]string{
				"id": "value-not-used",
			},
			expected: `'' "id"`,
		},
		{
			name: "multiple keys",
			primaryKey: map[string]string{
				"id":    "value-not-used",
				"block": "value-not-used",
				"idx":   "value-not-used",
			},
			expected: `'' "block",'' "id",'' "idx"`,
		},
		{
			name: "keys with special characters",
			primaryKey: map[string]string{
				"user_id":   "value-not-used",
				"order-num": "value-not-used",
			},
			expected: `'' "order-num",'' "user_id"`,
		},
		{
			name:       "empty map",
			primaryKey: map[string]string{},
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getPrimaryKeyFakeEmptyValues(tt.primaryKey)
			assert.Equal(t, tt.expected, result)

			// For multiple keys, verify the order is predictable (alphabetical)
			if len(tt.primaryKey) > 1 {
				parts := strings.Split(result, ",")
				for i := 1; i < len(parts); i++ {
					assert.True(t, strings.Compare(parts[i-1], parts[i]) <= 0,
						"Expected sorted keys, but got %s before %s", parts[i-1], parts[i])
				}
			}
		})
	}
}

func TestRisingwaveGetPrimaryKeyFakeEmptyValuesAssertion(t *testing.T) {
	tests := []struct {
		name             string
		primaryKey       map[string]string
		escapedTableName string
		expected         string
	}{
		{
			name: "single key",
			primaryKey: map[string]string{
				"id": "value-not-used",
			},
			escapedTableName: `"users"`,
			expected:         `"users"."id" IS NULL`,
		},
		{
			name: "multiple keys",
			primaryKey: map[string]string{
				"id":    "value-not-used",
				"block": "value-not-used",
				"idx":   "value-not-used",
			},
			escapedTableName: `"transactions"`,
			expected:         `"transactions"."block" IS NULL AND "transactions"."id" IS NULL AND "transactions"."idx" IS NULL`,
		},
		{
			name: "schema qualified table",
			primaryKey: map[string]string{
				"user_id": "value-not-used",
			},
			escapedTableName: `"public"."users"`,
			expected:         `"public"."users"."user_id" IS NULL`,
		},
		{
			name:             "empty map",
			primaryKey:       map[string]string{},
			escapedTableName: `"table"`,
			expected:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getPrimaryKeyFakeEmptyValuesAssertion(tt.primaryKey, tt.escapedTableName)
			assert.Equal(t, tt.expected, result)

			// For multiple keys, verify the order is predictable (alphabetical)
			if len(tt.primaryKey) > 1 {
				parts := strings.Split(result, "AND ")
				for i := 1; i < len(parts); i++ {
					assert.True(t, strings.Compare(parts[i-1], parts[i]) <= 0,
						"Expected sorted parts, but got %s before %s", parts[i-1], parts[i])
				}
			}
		})
	}
}
