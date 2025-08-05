package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name             string
		dns              string
		expectError      bool
		expectConnString string
		expectSchema     string
		expectPassword   string
	}{
		{
			name:             "golden path",
			dns:              "psql://postgres:postgres@localhost/substreams-dev?enable_incremental_sort=off&sslmode=disable",
			expectConnString: "host=localhost port=5432 dbname=substreams-dev enable_incremental_sort=off sslmode=disable user=postgres password=postgres",
			expectSchema:     "public",
			expectPassword:   "postgres",
		},
		{
			name:             "with schemaName",
			dns:              "psql://postgres:postgres@localhost/substreams-dev?enable_incremental_sort=off&sslmode=disable&schemaName=foo",
			expectConnString: "host=localhost port=5432 dbname=substreams-dev enable_incremental_sort=off  sslmode=disable user=postgres password=postgres",
			expectSchema:     "foo",
			expectPassword:   "postgres",
		},
		{
			name:             "with password",
			dns:              "clickhouse://default:password@localhost:9000/default",
			expectConnString: "clickhouse://default:password@localhost:9000/default",
			expectSchema:     "default",
			expectPassword:   "password",
		},
		{
			name:             "with blank password",
			dns:              "clickhouse://default:@localhost:9000/default",
			expectConnString: "clickhouse://default:@localhost:9000/default",
			expectSchema:     "default",
			expectPassword:   "",
		},
		{
			name:             "risingwave DSN",
			dns:              "risingwave://root@risingwave:4566/dev?sslmode=disable",
			expectConnString: "host=risingwave port=4566 dbname=dev sslmode=disable user=root",
			expectSchema:     "public",
			expectPassword:   "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, err := ParseDSN(test.dns)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectConnString, d.ConnString())
				assert.Equal(t, test.expectSchema, d.schema)
				assert.Equal(t, test.expectPassword, d.Password)
			}
		})
	}

}

func TestDSN_SqlDriver(t *testing.T) {
	tests := []struct {
		name              string
		dsn               string
		expectedDriver    string
		expectedSqlDriver string
	}{
		{
			name:              "postgres DSN",
			dsn:               "postgres://user:pass@localhost/db",
			expectedDriver:    "postgres",
			expectedSqlDriver: "postgres",
		},
		{
			name:              "risingwave DSN",
			dsn:               "risingwave://root@risingwave:4566/dev",
			expectedDriver:    "risingwave",
			expectedSqlDriver: "postgres",
		},
		{
			name:              "clickhouse DSN",
			dsn:               "clickhouse://default@localhost:9000/default",
			expectedDriver:    "clickhouse",
			expectedSqlDriver: "clickhouse",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, err := ParseDSN(test.dsn)
			require.NoError(t, err)
			assert.Equal(t, test.expectedDriver, d.Driver())
			assert.Equal(t, test.expectedSqlDriver, d.SqlDriver())
		})
	}
}
