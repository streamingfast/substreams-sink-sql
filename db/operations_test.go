package db

import (
	"context"
	"fmt"
	"github.com/bobg/go-generics/v2/slices"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
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

	createStatement :=
		fmt.Sprintf(`create table "test" (%s)`, strings.Join(slices.Map(colInputs, func(str string) string {
			return fmt.Sprintf("%s text", escapeString(str, "column"))
		}), ","))
	fmt.Println(createStatement)

	_, err = tx.ExecContext(ctx, createStatement)
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

			insertStatement := fmt.Sprintf(`insert into "test" ("col") values (%s);`, escapeString(str, "value"))
			_, err = tx.ExecContext(ctx, insertStatement)
			require.NoError(tt, err)

			fmt.Println(insertStatement)

			selectStatement := `select * from "test";`
			rows, err := tx.QueryContext(ctx, selectStatement)
			require.NoError(tt, err)

			for rows.Next() {
				var col string
				err = rows.Scan(&col)
				require.NoError(tt, err)
				fmt.Printf("%s:  %s\n\n", str, col)
			}

			deleteStatement := `delete from "test";`
			_, err = tx.ExecContext(ctx, deleteStatement)
			require.NoError(tt, err)

			err = tx.Commit()
			require.NoError(tt, err)
		})
	}
}
