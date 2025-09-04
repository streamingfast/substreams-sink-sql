package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrimaryKeyToJSON verifies deterministic JSON encoding of primary key maps:
// - single and multi-key support
// - stable lexical ordering of keys in output JSON
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

// TestJSONToPrimaryKey verifies decoding a JSON primary key back to a string map
// and preserves all keys/values regardless of input ordering.
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

// TestGetPrimaryKeyFakeEmptyValues verifies formatting of fake empty values used
// in history queries and that output is stable and lexically sorted for multi-keys.
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

// TestGetPrimaryKeyFakeEmptyValuesAssertion verifies the IS NULL assertion builder
// for one or multiple primary key columns, including schema-qualified table names.
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

// TestRevertOp validates SQL emitted to revert history operations:
// - I (insert) => DELETE target row
// - D (delete) => INSERT row from stored JSON
// - U (update) => UPDATE FROM json_populate_record of previous state
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
// These tests exercise buildUnnestInsertSQL for INSERT-only batches, ensuring:
// - scalar columns use typed ARRAY[...] with WITH ORDINALITY
// - array-typed columns are projected via CASE-by-ordinal with typed casts
// - NULL arrays and empty fallback ('{}'::type[]) are correctly emitted
// - an error is returned if there are no scalar columns to drive WITH ORDINALITY

// mkTestTable is a small helper to construct a table with given columns and PKs.
func mkTestTable(t *testing.T, name string, pk []string, cols map[string]*ColumnInfo) *TableInfo {
	t.Helper()
	tbl, err := NewTableInfo("public", name, pk, cols)
	require.NoError(t, err)
	return tbl
}

// Test_buildUnnestInsertSQL_MixedScalarAndArray verifies that the INSERT UNNEST builder:
// - emits typed arrays for scalars with WITH ORDINALITY aliases
// - selects array columns via CASE ((s.ord)::int) with per-row typed arrays
// - uses '{}'::varchar[] as the ELSE fallback and NULL::varchar[] when absent
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

// Test_buildUnnestInsertSQL_NoScalarColumns_Error verifies the guarded path that
// returns an error when a batch contains only array-typed columns (no scalars).
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

// --- 12b: UNNEST UPSERT with presence ---
// These tests exercise buildUnnestUpsertSQLWithPresence for UPSERT batches, ensuring:
// - per-column boolean[] presence arrays drive conditional updates in DO UPDATE
// - typed scalar value arrays and CASE-by-ordinal projections for array columns
// - default inlining occurs for presence=false in projection (both scalar and array)
// - tests avoid the 11c NOT NULL guard by making omitted columns nullable or defaulted

// Test_buildUnnestUpsertSQLWithPresence_Basics validates SQL shape for UNNEST UPSERT:
// - boolean presence arrays (::boolean[])
// - typed scalar arrays and WITH ORDINALITY
// - ON CONFLICT ... DO UPDATE uses presence via a subquery on src (c{idx}p)
func Test_buildUnnestUpsertSQLWithPresence_Basics(t *testing.T) {
	// Columns: age (INT8), id (INT8, PK), name (TEXT)
	age := NewColumnInfo("age", "INT8", int64(0))
	id := NewColumnInfo("id", "INT8", int64(0))
	name := NewColumnInfo("name", "TEXT", "")
	name.nullable = true
	cols := map[string]*ColumnInfo{
		"age":  age,
		"id":   id,
		"name": name,
	}
	tbl := mkTestTable(t, "users", []string{"id"}, cols)

	columnsEscaped := []string{`"age"`, `"id"`, `"name"`}
	perRowValues := [][]string{
		{"30", "1", "NULL"},  // name absent
		{"40", "2", "'bob'"}, // name present
	}
	perRowPresence := [][]bool{
		{true, true, false},
		{true, true, true},
	}

	sql, err := (&PostgresDialect{}).buildUnnestUpsertSQLWithPresence(tbl, columnsEscaped, perRowValues, perRowPresence)
	require.NoError(t, err)

	// Basic shape: INSERT ... SELECT ... FROM unnest(...) WITH ORDINALITY AS s(...)
	assert.Contains(t, sql, `INSERT INTO "public"."users" ("age","id","name") SELECT`)
	assert.Contains(t, sql, `WITH ORDINALITY AS s(`)
	// Presence arrays must be boolean[] alongside typed value arrays for scalars
	assert.Contains(t, sql, `::boolean[]`)
	assert.Contains(t, sql, `ARRAY[30,40]::bigint[]`)
	assert.Contains(t, sql, `ARRAY[1,2]::bigint[]`)
	// Upsert with presence-controlled updates
	assert.Contains(t, sql, `ON CONFLICT ("id") DO UPDATE SET`)
	// Update should reference presence via subquery on src with pk join; for name (index 2) expect c2p
	assert.Contains(t, sql, `CASE WHEN (SELECT src.c2p FROM src WHERE`)
}

// Test_buildUnnestUpsertSQLWithPresence_DefaultInlining validates that when presence=false
// the projection inlines defaults: scalar uses ELSE default, array uses ELSE default::type[].
func Test_buildUnnestUpsertSQLWithPresence_DefaultInlining(t *testing.T) {
	// Scalar default for score; array default for tags
	score := NewColumnInfo("score", "INT8", int64(0))
	score.hasDefault = true
	score.defaultExpr = "42"
	// tags is array-typed TEXT[] with '{}' default
	tags := NewColumnInfo("tags", "_TEXT", "")
	tags.hasDefault = true
	tags.defaultExpr = "'{}'"
	id := NewColumnInfo("id", "INT8", int64(0))
	cols := map[string]*ColumnInfo{
		"id":    id,
		"score": score,
		"tags":  tags,
	}
	tbl := mkTestTable(t, "users", []string{"id"}, cols)

	columnsEscaped := []string{`"score"`, `"id"`, `"tags"`}
	perRowValues := [][]string{
		{"NULL", "1", "NULL"},   // both defaults apply when absent
		{"100", "2", "'{x,y}'"}, // both present
	}
	perRowPresence := [][]bool{
		{false, true, false},
		{true, true, true},
	}

	sql, err := (&PostgresDialect{}).buildUnnestUpsertSQLWithPresence(tbl, columnsEscaped, perRowValues, perRowPresence)
	require.NoError(t, err)

	// Scalar default inlining for score: ELSE 42 in projection, typed to bigint
	assert.Contains(t, sql, `CASE WHEN s.p0 THEN (s.v0)::bigint ELSE 42 END`)
	// Array default inlining for tags: ELSE ('{}')::varchar[]
	assert.Contains(t, sql, `CASE WHEN s.p2 THEN`)
	assert.Contains(t, sql, `ELSE ('{}')::varchar[] END`)
}

// --- 12c: batch planning helpers ---
// These tests validate pre-SQL planning helpers (no SQL text assertions):
// - computeInsertBatchPlan: superset columns, PK inclusion, NULL filling, order
// - computeUpsertBatchPlan: identical column-set enforcement vs heterogeneous error
// - computeUpsertSupersetPlanWithPresence: superset, normalized values, presence matrix

// Test_computeInsertBatchPlan_SupersetAndNulls ensures superset columns across INSERT rows,
// deterministic sorted order, PK inclusion, and NULL for absent fields.
func Test_computeInsertBatchPlan_SupersetAndNulls(t *testing.T) {
	cols := map[string]*ColumnInfo{
		"id":   NewColumnInfo("id", "INT8", int64(0)),
		"name": NewColumnInfo("name", "TEXT", ""),
		"age":  NewColumnInfo("age", "INT8", int64(0)),
	}
	tbl := mkTestTable(t, "users", []string{"id"}, cols)

	ops := []*Operation{
		{opType: OperationTypeInsert, table: tbl, data: map[string]string{"id": "1", "name": "alice"}},
		{opType: OperationTypeInsert, table: tbl, data: map[string]string{"id": "2", "age": "30"}},
	}

	colsEsc, rows, err := (&PostgresDialect{}).computeInsertBatchPlan(ops)
	require.NoError(t, err)

	// Sorted by raw name: age, id, name
	assert.Equal(t, []string{`"age"`, `"id"`, `"name"`}, colsEsc)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{"NULL", "1", "'alice'"}, rows[0])
	assert.Equal(t, []string{"30", "2", "NULL"}, rows[1])
}

// Test_computeUpsertBatchPlan_IdenticalAndHeterogeneous verifies that UPSERT batches require
// identical column sets across rows and error out on heterogeneous sets.
func Test_computeUpsertBatchPlan_IdenticalAndHeterogeneous(t *testing.T) {
	cols := map[string]*ColumnInfo{
		"id":   NewColumnInfo("id", "INT8", int64(0)),
		"name": NewColumnInfo("name", "TEXT", ""),
		"age":  NewColumnInfo("age", "INT8", int64(0)),
	}
	tbl := mkTestTable(t, "users", []string{"id"}, cols)

	t.Run("identical_column_set", func(t *testing.T) {
		ops := []*Operation{
			{opType: OperationTypeUpsert, table: tbl, data: map[string]string{"id": "1", "name": "alice"}},
			{opType: OperationTypeUpsert, table: tbl, data: map[string]string{"id": "2", "name": "bob"}},
		}
		colsEsc, rows, err := (&PostgresDialect{}).computeUpsertBatchPlan(ops)
		require.NoError(t, err)
		assert.Equal(t, []string{`"id"`, `"name"`}, colsEsc)
		require.Len(t, rows, 2)
		assert.Equal(t, []string{"1", "'alice'"}, rows[0])
		assert.Equal(t, []string{"2", "'bob'"}, rows[1])
	})

	t.Run("heterogeneous_sets_error", func(t *testing.T) {
		ops := []*Operation{
			{opType: OperationTypeUpsert, table: tbl, data: map[string]string{"id": "1", "name": "alice"}},
			{opType: OperationTypeUpsert, table: tbl, data: map[string]string{"id": "2", "age": "30"}},
		}
		_, _, err := (&PostgresDialect{}).computeUpsertBatchPlan(ops)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "heterogeneous")
	})
}

// Test_computeUpsertSupersetPlanWithPresence_Basics validates superset columns are computed,
// values normalized, and presence matrix flags which columns were explicitly provided.
func Test_computeUpsertSupersetPlanWithPresence_Basics(t *testing.T) {
	cols := map[string]*ColumnInfo{
		"id":   NewColumnInfo("id", "INT8", int64(0)),
		"name": NewColumnInfo("name", "TEXT", ""),
		"age":  NewColumnInfo("age", "INT8", int64(0)),
	}
	tbl := mkTestTable(t, "users", []string{"id"}, cols)

	ops := []*Operation{
		{opType: OperationTypeUpsert, table: tbl, data: map[string]string{"id": "1", "name": "alice"}},
		{opType: OperationTypeUpsert, table: tbl, data: map[string]string{"id": "2", "age": "30"}},
	}

	colsEsc, vals, pres, err := (&PostgresDialect{}).computeUpsertSupersetPlanWithPresence(ops)
	require.NoError(t, err)
	assert.Equal(t, []string{`"age"`, `"id"`, `"name"`}, colsEsc)
	require.Len(t, vals, 2)
	require.Len(t, pres, 2)
	assert.Equal(t, []string{"NULL", "1", "'alice'"}, vals[0])
	assert.Equal(t, []bool{false, true, true}, pres[0])
	assert.Equal(t, []string{"30", "2", "NULL"}, vals[1])
	assert.Equal(t, []bool{true, true, false}, pres[1])
}
