package db

import (
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
