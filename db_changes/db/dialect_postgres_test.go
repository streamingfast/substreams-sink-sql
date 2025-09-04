package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKeyToJSON(t *testing.T) {

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

func TestJSONToPrimaryKey(t *testing.T) {

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

func TestGetPrimaryKeyFakeEmptyValues(t *testing.T) {
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

func TestGetPrimaryKeyFakeEmptyValuesAssertion(t *testing.T) {
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

func TestRevertOp(t *testing.T) {

	type row struct {
		op         string
		table_name string
		pk         string
		prev_value string
	}

	tests := []struct {
		name   string
		row    row
		expect string
	}{
		{
			name: "rollback insert row",
			row: row{
				op:         "I",
				table_name: `"testschema"."xfer"`,
				pk:         `{"id":"2345"}`,
				prev_value: "", // unused
			},
			expect: `DELETE FROM "testschema"."xfer" WHERE "id" = '2345';`,
		},
		{
			name: "rollback delete row",
			row: row{
				op:         "D",
				table_name: `"testschema"."xfer"`,
				pk:         `{"id":"2345"}`,
				prev_value: `{"id":"2345","sender":"0xdead","receiver":"0xbeef"}`,
			},
			expect: `INSERT INTO "testschema"."xfer" SELECT * FROM json_populate_record(null::"testschema"."xfer",` +
				`'{"id":"2345","sender":"0xdead","receiver":"0xbeef"}');`,
		},
		{
			name: "rollback update row",
			row: row{
				op:         "U",
				table_name: `"testschema"."xfer"`,
				pk:         `{"id":"2345"}`,
				prev_value: `{"id":"2345","sender":"0xdead","receiver":"0xbeef"}`,
			},
			expect: `UPDATE "testschema"."xfer" SET("id","receiver","sender")=((SELECT "id","receiver","sender" FROM json_populate_record(null::"testschema"."xfer",` +
				`'{"id":"2345","sender":"0xdead","receiver":"0xbeef"}'))) WHERE "id" = '2345';`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := &TestTx{}
			ctx := context.Background()
			pd := PostgresDialect{}

			row := test.row
			err := pd.revertOp(tx, ctx, row.op, row.table_name, row.pk, row.prev_value, 9999)
			require.NoError(t, err)
			assert.Equal(t, []string{test.expect}, tx.Results())
		})
	}

}

// --- 12a: UNNEST INSERT builder tests ---

func mkTestTable(t *testing.T, name string, pk []string, cols map[string]*ColumnInfo) *TableInfo {
	t.Helper()
	tbl, err := NewTableInfo("public", name, pk, cols)
	require.NoError(t, err)
	return tbl
}

func Test_buildUnnestInsertSQL_MixedScalarAndArray(t *testing.T) {
	cols := map[string]*ColumnInfo{
		"id":     NewColumnInfo("id", "INT8", int64(0)), // bigint
		"amount": NewColumnInfo("amount", "NUMERIC", float64(0)),
		"tags":   NewColumnInfo("tags", "_TEXT", ""), // text[]
	}
	tbl := mkTestTable(t, "xfer", []string{"id"}, cols)

	// columnsEscaped must match ColumnInfo.escapedName values
	columnsEscaped := []string{`"amount"`, `"id"`, `"tags"`}
	perRowValues := [][]string{
		{"12.34", "1", "'{a,b}'"}, // tags present
		{"56.78", "2", "NULL"},    // tags absent -> NULL
	}

	sql, err := (&PostgresDialect{}).buildUnnestInsertSQL(tbl, columnsEscaped, perRowValues)
	require.NoError(t, err)

	// Basic shape
	assert.Contains(t, sql, `INSERT INTO "public"."xfer" ("amount","id","tags") SELECT`)
	// Scalars become typed arrays and use WITH ORDINALITY
	assert.Contains(t, sql, `unnest(ARRAY[12.34,56.78]::numeric[], ARRAY[1,2]::bigint[]) WITH ORDINALITY AS s(c0,c1,ord)`)
	// Array column uses CASE-by-ordinal with typed casts and empty fallback
	assert.Contains(t, sql, `CASE ((s.ord)::int)`)
	assert.Contains(t, sql, `WHEN 1 THEN`)
	assert.Contains(t, sql, `WHEN 2 THEN`)
	assert.Contains(t, sql, `ELSE '{}'::varchar[] END`)
	// NULL array in row 2 should render as NULL::varchar[] in a CASE arm
	assert.Contains(t, sql, `NULL::varchar[]`)
}

func Test_buildUnnestInsertSQL_NoScalarColumns_Error(t *testing.T) {
	cols := map[string]*ColumnInfo{
		// contrived: only array-typed column (also PK)
		"keyarr": NewColumnInfo("keyarr", "_INT8", int64(0)), // bigint[]
	}
	tbl := mkTestTable(t, "arr_only", []string{"keyarr"}, cols)

	columnsEscaped := []string{`"keyarr"`}
	perRowValues := [][]string{
		{"'{1,2}'"},
		{"'{3,4}'"},
	}

	_, err := (&PostgresDialect{}).buildUnnestInsertSQL(tbl, columnsEscaped, perRowValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no scalar columns")
}
