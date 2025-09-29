package dsn

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
			expectConnString: "host=localhost port=5432 dbname=substreams-dev enable_incremental_sort=off sslmode=disable user=postgres password=postgres",
			expectSchema:     "foo",
			expectPassword:   "postgres",
		},
		{
			name:             "with password",
			dns:              "clickhouse://default:password@host.1:9000/default",
			expectConnString: "clickhouse://default:password@host.1:9000/default",
			expectSchema:     "",
			expectPassword:   "password",
		},
		{
			name:             "with blank password",
			dns:              "clickhouse://default:@host.1:9000/default",
			expectConnString: "clickhouse://default:@host.1:9000/default",
			expectSchema:     "",
			expectPassword:   "",
		},
		{
			name:             "clickhouse DSN weird code, if option present and host is localhost, it changes the scheme to http and host to 127.0.0.1",
			dns:              "clickhouse://default:password@localhost:9000/default?any=option",
			expectConnString: "http://default:password@127.0.0.1:9000/default?any=option",
			expectSchema:     "",
			expectPassword:   "password",
		},
		{
			name:             "clickhouse DSN weird code, if option present and host is localhost and secure=true option, it changes the scheme to https and host to 127.0.0.1",
			dns:              "clickhouse://default:password@localhost:9000/default?secure=true",
			expectConnString: "https://default:password@127.0.0.1:9000/default?secure=true",
			expectSchema:     "",
			expectPassword:   "password",
		},
		{
			name:             "clickhouse DSN weird code, if option present and host is NOT localhost, nothing changes",
			dns:              "clickhouse://default:password@host:9000/default?any=option",
			expectConnString: "clickhouse://default:password@host:9000/default?any=option",
			expectSchema:     "",
			expectPassword:   "password",
		},
		{
			name:             "clickhouse DSN weird code, if option present and host is NOT localhost and secure=true option, nothing changes",
			dns:              "clickhouse://default:password@host:9000/default?secure=true",
			expectConnString: "clickhouse://default:password@host:9000/default?secure=true",
			expectSchema:     "",
			expectPassword:   "password",
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
