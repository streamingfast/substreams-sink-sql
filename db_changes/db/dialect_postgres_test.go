package db

import (
	"context"
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
