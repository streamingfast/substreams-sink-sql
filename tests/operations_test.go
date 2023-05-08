package tests

import (
	"github.com/streamingfast/substreams-sink-postgres/db"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEscapeString(t *testing.T) {
	var tests = []struct {
		stringIn       string
		escapeType     string
		stringExpected string
	}{
		{
			stringIn:       "regular-column",
			escapeType:     "column",
			stringExpected: "regular-column",
		}, {
			stringIn:       "from",
			escapeType:     "column",
			stringExpected: "\"from\"",
		}, {
			stringIn:       "columnTransfer's",
			escapeType:     "column",
			stringExpected: "columnTransfer\\'s",
		}, {
			stringIn:       "big \"transactions\"",
			escapeType:     "column",
			stringExpected: "big \\\"transactions\\\"",
		}, {
			stringIn:       "table\nvaluesColumn",
			escapeType:     "column",
			stringExpected: "table\\\nvaluesColumn",
		},

		{
			stringIn:       "regular-value",
			escapeType:     "value",
			stringExpected: "'regular-value'",
		}, {
			stringIn:       "from",
			escapeType:     "value",
			stringExpected: "'from'",
		}, {
			stringIn:       "valueTransfer's",
			escapeType:     "value",
			stringExpected: "'valueTransfer\\'s'",
		}, {
			stringIn:       "big \"transactions\"",
			escapeType:     "value",
			stringExpected: "'big \\\"transactions\\\"'",
		}, {
			stringIn:       "table\nvaluesValue",
			escapeType:     "value",
			stringExpected: "'table\\\\\nvaluesValue'",
		},
	}

	for i, test := range tests {
		escapedString, err := db.EscapeString(test.stringIn, test.escapeType)
		if err != nil {
			t.Errorf("test num %v: unable to escape string %s: %s", i, test.stringIn, err)
		}

		require.Equal(t, test.stringExpected, escapedString)
	}
}
