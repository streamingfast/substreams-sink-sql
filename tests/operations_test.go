package tests

import (
	"fmt"
	"github.com/streamingfast/substreams-sink-postgres/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os/exec"
	"testing"
)

func TestEscapeStringIntegration(t *testing.T) {
	t.Skip()

	var tests = []struct {
		name          string
		values        []string
		escapedValues []string
		wantErr       assert.ErrorAssertionFunc
	}{
		{
			name:          "proper",
			values:        []string{"parent-hash", "tableValue"},
			escapedValues: []string{"parent-hash", "tableValue"},
		},
		{
			name:          "reserved column name",
			values:        []string{"from", "tableValue"},
			escapedValues: []string{"\"from\"", "tableValue"},
		},
		{
			name:          "escape character",
			values:        []string{"parent-hash", "table\nValue"},
			escapedValues: []string{"parent-hash", "table\nValue"},
		},
		{
			name:          "single quote",
			values:        []string{"parent's-hash", "ta'ble'Value"},
			escapedValues: []string{"parent\\'s-hash", "ta\\'ble\\'Value"},
		},
		{
			name:          "double quote",
			values:        []string{"pa\"rent\"-hash", "table\"Value\""},
			escapedValues: []string{"pa\\\"rent\\\"-hash", "table\\\"Value\\\""},
		},
	}

	for _, test := range tests {
		cmd, err := exec.Command("substreams-sink-postgres", "run",
			"psql://postgres:password@localhost:5432/block_sink_meta?sslmode=disable",
			"mainnet.eth.streamingfast.io:443",
			"./../docs/tutorial/substreams.yaml",
			"db_out",
		).Output()
		//cmd, err := exec.Command("pwd").Output()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(cmd), test)
		//		sqlStatement := `
		//
		//`
	}
}

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
			stringExpected: "'table\\\nvaluesValue'",
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
