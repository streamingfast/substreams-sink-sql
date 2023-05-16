package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bobg/go-generics/v2/slices"
	"github.com/stretchr/testify/require"
)

func TestEscapeColumns(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip(`PG_DSN not set, please specify PG_DSN to run this test, example: PG_DSN="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?enable_incremental_sort=off&sslmode=disable"`)
	}

	dbLoader, err := NewLoader(dsn, 0, zlog, tracer)
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
		return fmt.Sprintf("%s text", escapeIdentifier(str))
	}), ",")

	createStatement := fmt.Sprintf(`create table "test" (%s)`, columnDefs)
	_, err = tx.ExecContext(ctx, createStatement)
	require.NoError(t, err)

	columns := strings.Join(slices.Map(colInputs, escapeIdentifier), ",")
	values := strings.Join(slices.Map(colInputs, func(str string) string { return `'any'` }), ",")
	insertStatement := fmt.Sprintf(`insert into "test" (%s) values (%s)`, columns, values)

	_, err = tx.ExecContext(ctx, insertStatement)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)
}

func TestEscapeValues(t *testing.T) {

	ctx := context.Background()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip(`PG_DSN not set, please specify PG_DSN to run this test, example: PG_DSN="psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?enable_incremental_sort=off&sslmode=disable"`)
	}

	dbLoader, err := NewLoader(dsn, 0, zlog, tracer)
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
