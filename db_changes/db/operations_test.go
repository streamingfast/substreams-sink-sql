package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bobg/go-generics/v2/slices"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEscapeColumns(t *testing.T) {
	ctx := context.Background()
	dsnString := os.Getenv("PG_DSN")
	if dsnString == "" {
		t.Skip(`PG_DSN not set, please specify PG_DSN to run this test, example: PG_DSN="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?enable_incremental_sort=off&sslmode=disable"`)
	}

	dsn, err := ParseDSN(dsnString)
	require.NoError(t, err)

	dbLoader, err := NewLoader(
		dsn,
		testCursorTableName,
		testHistoryTableName,
		"cluster.name.1",
		0, 0, 0,
		OnModuleHashMismatchIgnore.String(),
		nil,
		zlog, tracer,
	)
	require.NoError(t, err)

	tx, err := dbLoader.DB.Begin()
	require.NoError(t, err)

	colInputs := []string{
		"regular",

		"from", // reserved keyword

		"withnewline\nafter",
		"withtab\tafter",
		"withreturn\rafter",
		"withbackspace\bafter",
		"withformfeed\fafter",

		`withdoubleQuote"aftersdf`,
		`withbackslash\after`,
		`withsinglequote'after`,
	}

	columnDefs := strings.Join(slices.Map(colInputs, func(str string) string {
		return fmt.Sprintf("%s text", EscapeIdentifier(str))
	}), ",")

	createStatement := fmt.Sprintf(`create table "test" (%s)`, columnDefs)
	_, err = tx.ExecContext(ctx, createStatement)
	require.NoError(t, err)

	columns := strings.Join(slices.Map(colInputs, EscapeIdentifier), ",")
	values := strings.Join(slices.Map(colInputs, func(str string) string { return `'any'` }), ",")
	insertStatement := fmt.Sprintf(`insert into "test" (%s) values (%s)`, columns, values)

	_, err = tx.ExecContext(ctx, insertStatement)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)
}

func TestEscapeValues(t *testing.T) {

	ctx := context.Background()
	dsnString := os.Getenv("PG_DSN")
	if dsnString == "" {
		t.Skip(`PG_DSN not set, please specify PG_DSN to run this test, example: PG_DSN="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?enable_incremental_sort=off&sslmode=disable"`)
	}

	dsn, err := ParseDSN(dsnString)
	require.NoError(t, err)

	dbLoader, err := NewLoader(
		dsn,
		testCursorTableName,
		testHistoryTableName,
		"cluster.name.1",
		0, 0, 0,
		OnModuleHashMismatchIgnore.String(),
		nil,
		zlog, tracer,
	)
	require.NoError(t, err)

	tx, err := dbLoader.DB.Begin()
	require.NoError(t, err)

	createStatement := `create table "test" ("col" text);`
	_, err = tx.ExecContext(ctx, createStatement)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	defer func() {
		_, err = dbLoader.DB.ExecContext(ctx, `drop table "test"`)
		require.NoError(t, err)
	}()

	valueStrings := []string{
		`regularValue`,

		`withApostrophe'`,

		"withNewlineCharNone\nafter",
		"withTabCharNone\tafter",
		"withCarriageReturnCharNone\rafter",
		"withBackspaceCharNone\bafter",
		"withFormFeedCharNone\fafter",

		`with\nNewlineLiteral`,

		`with'singleQuote`,
		`withDoubleQuote"`,
		`withSingle\Backslash`,

		`withExoticCharacterNone中文`,
	}

	for _, str := range valueStrings {
		t.Run(str, func(tt *testing.T) {

			tx, err := dbLoader.DB.Begin()
			require.NoError(t, err)

			insertStatement := fmt.Sprintf(`insert into "test" ("col") values (%s);`, escapeStringValue(str))
			_, err = tx.ExecContext(ctx, insertStatement)
			require.NoError(tt, err)

			checkStatement := `select "col" from "test";`
			row := tx.QueryRowContext(ctx, checkStatement)
			var value string
			err = row.Scan(&value)
			require.NoError(tt, err)
			require.Equal(tt, str, value, "Inserted value is not equal to the expected value")

			err = tx.Rollback()
			require.NoError(tt, err)
		})
	}
}

func Test_prepareColValues(t *testing.T) {
	type args struct {
		table     *TableInfo
		colValues map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantColumns []string
		wantValues  []string
		assertion   require.ErrorAssertionFunc
	}{
		{
			"bool true",
			args{
				newTable(t, "schemaName", "name", "id", NewColumnInfo("col", "bool", true)),
				map[string]string{"col": "true"},
			},
			[]string{`"col"`},
			[]string{`'true'`},
			require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialect := PostgresDialect{}

			gotColumns, gotValues, err := dialect.prepareColValues(tt.args.table, tt.args.colValues)
			tt.assertion(t, err)
			assert.Equal(t, tt.wantColumns, gotColumns)
			assert.Equal(t, tt.wantValues, gotValues)
		})
	}
}

func newTable(t *testing.T, schema, name, primaryColumn string, columnInfos ...*ColumnInfo) *TableInfo {
	columns := make(map[string]*ColumnInfo)
	columns[primaryColumn] = NewColumnInfo(primaryColumn, "text", "")
	for _, columnInfo := range columnInfos {
		columns[columnInfo.name] = columnInfo
	}

	table, err := NewTableInfo("public", "data", []string{"id"}, columns)
	require.NoError(t, err)

	return table
}
