package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
	pbdatabase "github.com/streamingfast/substreams-sink-database-changes/pb/sf/substreams/sink/database/v1"
	db2 "github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams-sink-sql/db_changes/sinker"
	pbsql "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/services/v1"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/types/known/anypb"
)

const testSchema = dbChangesSchemaName

func TestSinker_Integration_SinglePrimaryKey(t *testing.T) {
	testTables := db2.TestSinglePrimaryKeyTables(testSchema)
	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t)

	type XferSinglePKRow struct {
		ID   string `db:"id"`
		From string `db:"from"`
		To   string `db:"to"`
	}

	equalsXferRows := func(expected []*XferSinglePKRow) func(t *testing.T, dbx *sqlx.DB) {
		return func(t *testing.T, dbx *sqlx.DB) {
			require.Equal(t, expected, readDbChangesRows[XferSinglePKRow](t, dbx, "xfer"))
		}
	}

	tests := []struct {
		name                string
		responses           []*pbsubstreamsrpc.Response
		expected            func(t *testing.T, dbx *sqlx.DB)
		expectedFinalCursor string
	}{
		{
			"insert final",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("10a"),
					insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1"),
				),
			),
			equalsXferRows([]*XferSinglePKRow{
				{ID: "1234", From: "sender1", To: "receiver1"},
			}),
			"Block #10 (10a) - LIB #10 (10a)",
		},
		{
			"insert then undo insertion",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1"),
				),
				blockUndo(t, "9a", finalBlock("8a")),
			),
			nil,
			"Block #9 (9a) - LIB #8 (8a)",
		},

		{
			"upsert final",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("10a"),
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
			),
			equalsXferRows([]*XferSinglePKRow{
				{ID: "1234", From: "sender2", To: "receiver2"},
			}),
			"Block #10 (10a) - LIB #10 (10a)",
		},
		{
			"upsert, first insert, second update, final",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", finalBlock("11a"),
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
			),
			equalsXferRows([]*XferSinglePKRow{
				{ID: "1234", From: "sender2", To: "receiver3"},
			}),
			"Block #11 (11a) - LIB #11 (11a)",
		},

		{
			"upsert, first insert, undo insertion",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				blockUndo(t, "9a", finalBlock("8a")),
			),
			nil,
			"Block #9 (9a) - LIB #8 (8a)",
		},
		{
			"upsert, first insert, second update, undo initial insert",
			streamMock(
				dbChangesBlockData(t, "10a", "8a",
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", "8a",
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
				blockUndo(t, "9a", finalBlock("8a")),
			),
			nil,
			"Block #9 (9a) - LIB #8 (8a)",
		},
		{
			"upsert, first insert, second update, undo last update",
			streamMock(
				dbChangesBlockData(t, "10a", "8a",
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", "8a",
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
				blockUndo(t, "10a", finalBlock("8a")),
			),
			equalsXferRows([]*XferSinglePKRow{
				{ID: "1234", From: "sender2", To: "receiver2"},
			}),
			"Block #10 (10a) - LIB #8 (8a)",
		},
		{
			"upsert, first insert, second update, third update, undo last update",
			streamMock(
				dbChangesBlockData(t, "10a", "8a",
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", "8a",
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
				dbChangesBlockData(t, "12a", "8a",
					upsertRowSinglePK("xfer", "1234", "from", "sender3"),
				),
				blockUndo(t, "11a", finalBlock("8a")),
			),
			equalsXferRows([]*XferSinglePKRow{
				{ID: "1234", From: "sender2", To: "receiver3"},
			}),
			"Block #11 (11a) - LIB #8 (8a)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runSinkerTest(
				t,
				tablesInput(testTables),
				dbConnectionString,
				postgresContainer,
				test.responses,
				test.expected,
				test.expectedFinalCursor,
			)
		})
	}
}

func TestSinker_Integration_CompositePrimaryKey(t *testing.T) {
	testTables := db2.TestTables(testSchema, map[string]*db2.TableInfo{
		"xfer": mustNewTableInfo(testSchema, "xfer", []string{"id", "number"}, map[string]*db2.ColumnInfo{
			"id":     db2.NewColumnInfo("id", "text", ""),
			"number": db2.NewColumnInfo("number", "bigint", ""),
			"from":   db2.NewColumnInfo("from", "text", ""),
			"to":     db2.NewColumnInfo("to", "text", ""),
		}),
	})

	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t)

	pk := compositePK

	type XferCompositePKRow struct {
		ID     string `db:"id"`
		Number string `db:"number"`
		From   string `db:"from"`
		To     string `db:"to"`
	}

	equalsXferCompositePKRows := func(expected []*XferCompositePKRow) func(t *testing.T, dbx *sqlx.DB) {
		return func(t *testing.T, dbx *sqlx.DB) {
			require.Equal(t, expected, readDbChangesRows[XferCompositePKRow](t, dbx, "xfer"))
		}
	}

	tests := []struct {
		name                string
		responses           []*pbsubstreamsrpc.Response
		expected            func(t *testing.T, dbx *sqlx.DB)
		expectedFinalCursor string
	}{
		{
			"insert final",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("10a"),
					insertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender1", "to", "receiver1"),
				),
			),
			equalsXferCompositePKRows([]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender1", To: "receiver1"},
			}),
			"Block #10 (10a) - LIB #10 (10a)",
		},
		{
			"insert then undo insertion",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					insertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender1", "to", "receiver1"),
				),
				blockUndo(t, "9a", finalBlock("8a")),
			),
			nil,
			"Block #9 (9a) - LIB #8 (8a)",
		},

		{
			"upsert final",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("10a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
			),
			equalsXferCompositePKRows([]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender2", To: "receiver2"},
			}),
			"Block #10 (10a) - LIB #10 (10a)",
		},
		{
			"upsert, first insert, second update, final",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", finalBlock("11a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver3"),
				),
			),
			equalsXferCompositePKRows([]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender2", To: "receiver3"},
			}),
			"Block #11 (11a) - LIB #11 (11a)",
		},

		{
			"upsert, first insert, undo insertion",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				blockUndo(t, "9a", finalBlock("8a")),
			),
			nil,
			"Block #9 (9a) - LIB #8 (8a)",
		},
		{
			"upsert, first insert, second update, undo initial insert",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver3"),
				),
				blockUndo(t, "9a", finalBlock("8a")),
			),
			nil,
			"Block #9 (9a) - LIB #8 (8a)",
		},
		{
			"upsert, first insert, second update, undo last update",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver3"),
				),
				blockUndo(t, "10a", finalBlock("8a")),
			),
			equalsXferCompositePKRows([]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender2", To: "receiver2"},
			}),
			"Block #10 (10a) - LIB #8 (8a)",
		},
		{
			"upsert, first insert, second update, third update, undo last update",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver3"),
				),
				dbChangesBlockData(t, "12a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender3"),
				),
				blockUndo(t, "11a", finalBlock("8a")),
			),
			equalsXferCompositePKRows([]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender2", To: "receiver3"},
			}),
			"Block #11 (11a) - LIB #8 (8a)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runSinkerTest(
				t,
				tablesInput(testTables),
				dbConnectionString,
				postgresContainer,
				test.responses,
				test.expected,
				test.expectedFinalCursor,
			)
		})
	}
}

func TestSinker_Integration_CompositePrimaryKey_Delete(t *testing.T) {
	testTables := db2.TestTables(testSchema, map[string]*db2.TableInfo{
		"xfer": mustNewTableInfo(testSchema, "xfer", []string{"id", "number"}, map[string]*db2.ColumnInfo{
			"id":     db2.NewColumnInfo("id", "text", ""),
			"number": db2.NewColumnInfo("number", "bigint", ""),
			"from":   db2.NewColumnInfo("from", "text", ""),
			"to":     db2.NewColumnInfo("to", "text", ""),
		}),
	})

	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t)

	pk := compositePK

	type XferCompositePKRow struct {
		ID     string `db:"id"`
		Number string `db:"number"`
		From   string `db:"from"`
		To     string `db:"to"`
	}

	equalsXferCompositePKRows := func(expected []*XferCompositePKRow) func(t *testing.T, dbx *sqlx.DB) {
		return func(t *testing.T, dbx *sqlx.DB) {
			require.Equal(t, expected, readDbChangesRows[XferCompositePKRow](t, dbx, "xfer"))
		}
	}

	tests := []struct {
		name                string
		responses           []*pbsubstreamsrpc.Response
		expected            func(t *testing.T, dbx *sqlx.DB)
		expectedFinalCursor string
	}{
		{
			"insert then delete - composite primary key",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					insertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender1", "to", "receiver1"),
				),
				dbChangesBlockData(t, "11a", finalBlock("11a"),
					deleteRowMultiplePK("xfer", pk("id", "12", "number", "34")),
				),
			),
			func(t *testing.T, dbx *sqlx.DB) {
				require.Empty(t, readDbChangesRows[XferCompositePKRow](t, dbx, "xfer"))
			},
			"Block #11 (11a) - LIB #11 (11a)",
		},
		{
			"insert, update, then delete - composite primary key",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					insertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender1", "to", "receiver1"),
				),
				dbChangesBlockData(t, "11a", finalBlock("8a"),
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver2"),
				),
				dbChangesBlockData(t, "12a", finalBlock("12a"),
					deleteRowMultiplePK("xfer", pk("id", "12", "number", "34")),
				),
			),
			func(t *testing.T, dbx *sqlx.DB) {
				require.Empty(t, readDbChangesRows[XferCompositePKRow](t, dbx, "xfer"))
			},
			"Block #12 (12a) - LIB #12 (12a)",
		},
		{
			"multiple inserts with different composite keys, delete one",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					insertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender1", "to", "receiver1"),
					insertRowMultiplePK("xfer", pk("id", "56", "number", "78"), "from", "sender2", "to", "receiver2"),
				),
				dbChangesBlockData(t, "11a", finalBlock("11a"),
					deleteRowMultiplePK("xfer", pk("id", "12", "number", "34")),
				),
			),
			equalsXferCompositePKRows([]*XferCompositePKRow{
				{ID: "56", Number: "78", From: "sender2", To: "receiver2"},
			}),
			"Block #11 (11a) - LIB #11 (11a)",
		},
		{
			"insert then delete with undo",
			streamMock(
				dbChangesBlockData(t, "10a", finalBlock("8a"),
					insertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender1", "to", "receiver1"),
				),
				dbChangesBlockData(t, "11a", finalBlock("8a"),
					deleteRowMultiplePK("xfer", pk("id", "12", "number", "34")),
				),
				blockUndo(t, "10a", finalBlock("8a")),
			),
			equalsXferCompositePKRows([]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender1", To: "receiver1"},
			}),
			"Block #10 (10a) - LIB #8 (8a)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runSinkerTest(
				t,
				tablesInput(testTables),
				dbConnectionString,
				postgresContainer,
				test.responses,
				test.expected,
				test.expectedFinalCursor,
			)
		})
	}
}

func TestSinker_Integration_Bytes(t *testing.T) {
	schema := testSchema
	testTables := db2.TestTables(schema, map[string]*db2.TableInfo{
		"xfer": mustNewTableInfo(schema, "xfer", []string{"id"}, map[string]*db2.ColumnInfo{
			"id":    db2.NewColumnInfo("id", "bytea", []byte{}),
			"value": db2.NewColumnInfo("value", "bytea", []byte{}),
		}),
	})

	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t)

	type XferBytesRow struct {
		ID    []byte `db:"id"`
		Value []byte `db:"value"`
	}

	runSinkerTest(
		t,
		tablesInput(testTables),
		dbConnectionString,
		postgresContainer,
		streamMock(
			dbChangesBlockData(t, "10a", finalBlock("10a"),
				insertRowSinglePK("xfer", `\x01`, "value", `\x04ab`),
			),
		),
		func(t *testing.T, dbx *sqlx.DB) {
			require.Equal(t,
				[]*XferBytesRow{{ID: []byte{0x01}, Value: []byte{0x04, 0xab}}},
				readDbChangesRows[XferBytesRow](t, dbx, "xfer"),
			)
		},
		"Block #10 (10a) - LIB #10 (10a)",
	)
}

func TestSinker_Integration_TimescaleDB(t *testing.T) {
	schema := testSchema
	testTables := db2.TestTables(schema, map[string]*db2.TableInfo{
		"xfer": mustNewTableInfo(schema, "xfer", []string{"id"}, map[string]*db2.ColumnInfo{
			"id":    db2.NewColumnInfo("id", "bytea", []byte{}),
			"value": db2.NewColumnInfo("value", "bytea", []byte{}),
		}),
	})

	dbConnectionString, postgresContainer := setupDbChangesTimescaleDBContainer(t)

	type XferBytesRow struct {
		ID    []byte `db:"id"`
		Value []byte `db:"value"`
	}

	runSinkerTest(
		t,
		tablesInput(testTables),
		dbConnectionString,
		postgresContainer,
		streamMock(
			dbChangesBlockData(t, "10a", finalBlock("10a"),
				insertRowSinglePK("xfer", `\x01`, "value", `\x04ab`),
			),
		),
		func(t *testing.T, dbx *sqlx.DB) {
			require.Equal(t,
				[]*XferBytesRow{{ID: []byte{0x01}, Value: []byte{0x04, 0xab}}},
				readDbChangesRows[XferBytesRow](t, dbx, "xfer"),
			)
		},
		"Block #10 (10a) - LIB #10 (10a)",
	)
}

func TestSinker_Integration_ParentChildOrdering(t *testing.T) {
	sqlSchema := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.users (
			id TEXT PRIMARY KEY
		);
		CREATE TABLE IF NOT EXISTS %s.xfer (
			id TEXT PRIMARY KEY,
			"from" TEXT,
			CONSTRAINT fk_users
				FOREIGN KEY("from")
				REFERENCES %s.users(id)
		);
	`, testSchema, testSchema, testSchema)

	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t)

	type XferRow struct {
		ID   string `db:"id"`
		From string `db:"from"`
	}

	type UserRow struct {
		ID string `db:"id"`
	}

	runSinkerTest(
		t,
		rawSQLInput(sqlSchema),
		dbConnectionString,
		postgresContainer,
		streamMock(
			dbChangesBlockData(t, "10a", finalBlock("10a"),
				insertRowSinglePK("users", "user1"),
				insertRowSinglePK("xfer", "xfer1", "from", "user1"),
			),
		),
		func(t *testing.T, dbx *sqlx.DB) {
			require.Equal(t,
				[]*XferRow{{ID: "xfer1", From: "user1"}},
				readDbChangesRows[XferRow](t, dbx, "xfer"),
			)
		},
		"Block #10 (10a) - LIB #10 (10a)",
	)
}

func runSinkerTest(
	t *testing.T,
	setupInput sinkerSetupInput,
	dbDSN string,
	postgresContainer *postgres.PostgresContainer,
	responses []*pbsubstreamsrpc.Response,
	expected func(t *testing.T, dbx *sqlx.DB),
	expectedFinalCursor string,
) {
	t.Helper()

	ctx := context.Background()
	t.Cleanup(func() {
		require.NoError(t, postgresContainer.Restore(ctx))
	})

	// Setup fake substreams server and test package
	pattern := make([]interface{}, len(responses))
	for i, resp := range responses {
		pattern[i] = resp
	}
	substreamsClientConfig := setupFakeSubstreamsServer(t, pattern...)
	spkg := substreamsTestPackage(pbdatabase.File_sf_substreams_sink_database_v1_database_proto, (*pbdatabase.DatabaseChanges)(nil).ProtoReflect().Descriptor())

	var err error
	spkg.SinkConfig, err = anypb.New(&pbsql.Service{
		Schema: setupInput.ToSQL(),
	})
	require.NoError(t, err)

	setupOptions := sinker.SinkerSetupOptions{
		CursorTableName:            "cursors",
		HistoryTableName:           "history",
		ClickhouseCluster:          "",
		OnModuleHashMismatch:       "error",
		SystemTablesOnly:           false,
		IgnoreDuplicateTableErrors: false,
		Postgraphile:               false,
	}

	err = sinker.SinkerSetup(ctx, dbDSN, spkg, setupOptions, logger, tracer)
	require.NoError(t, err)

	// Load table metadata (including cursor table) - this is required for InsertCursor to work
	baseSink, err := sink.New(
		sink.SubstreamsModeProduction,
		false,
		spkg,
		spkg.Modules.Modules[0],
		manifest.ModuleHash{},
		substreamsClientConfig,
		logger,
		tracer,
		sink.WithBlockRange(bstream.MustParseRange("1-1000", bstream.WithExclusiveEnd())),
		sink.WithLivenessChecker(&isAlwaysLiveChecker{}),
	)
	require.NoError(t, err)

	// Create sinker factory options
	options := sinker.SinkerFactoryOptions{
		CursorTableName:         setupOptions.CursorTableName,
		HistoryTableName:        setupOptions.HistoryTableName,
		ClickhouseCluster:       setupOptions.ClickhouseCluster,
		BatchBlockFlushInterval: 1,
		BatchRowFlushInterval:   3,
		LiveBlockFlushInterval:  1,
		OnModuleHashMismatch:    setupOptions.OnModuleHashMismatch,
		HandleReorgs:            true,
		FlushRetryCount:         3,
		FlushRetryDelay:         1 * time.Second,
	}

	dbSinker, err := sinker.SinkerFactory(baseSink, options)(ctx, dbDSN, logger, tracer)
	require.NoError(t, err)
	t.Cleanup(func() { dbSinker.Close() })

	dbSinker.Run(ctx)
	require.NoError(t, dbSinker.Err())

	cleanDSN := strings.Replace(dbDSN, "&schemaName=testschema", "", 1)
	db, err := sqlx.Connect("postgres", cleanDSN)
	require.NoError(t, err)
	defer db.Close()

	if expected != nil {
		expected(t, db)
	}

	// Fetch cursor directly from database instead of using GetCursor
	var cursorStr string
	cursorQuery := fmt.Sprintf(`SELECT cursor FROM "%s"."cursors" WHERE id = $1`, testSchema)
	err = db.GetContext(ctx, &cursorStr, cursorQuery, dbSinker.OutputModuleHash())
	require.NoError(t, err)

	finalCursor, err := bstream.CursorFromOpaque(cursorStr)
	require.NoError(t, err)

	actualCursor := fmt.Sprintf("Block %s - LIB %s", finalCursor.Block, finalCursor.LIB)
	require.Equal(t, expectedFinalCursor, actualCursor)
}

func getFields(fieldsAndValues ...string) (out []*pbdatabase.Field) {
	if len(fieldsAndValues)%2 != 0 {
		panic("tableChangeSinglePK needs even number of fieldsAndValues")
	}
	for i := 0; i < len(fieldsAndValues); i += 2 {
		out = append(out, &pbdatabase.Field{
			Name:     fieldsAndValues[i],
			NewValue: fieldsAndValues[i+1],
		})
	}
	return
}

func compositePK(keyValuePairs ...string) map[string]string {
	if len(keyValuePairs)%2 != 0 {
		panic("compositePK needs even number of keyValuePairs")
	}
	out := make(map[string]string)
	for i := 0; i < len(keyValuePairs); i += 2 {
		out[keyValuePairs[i]] = keyValuePairs[i+1]
	}
	return out
}

func insertRowSinglePK(table string, pk string, fieldsAndValues ...string) *pbdatabase.TableChange {
	return &pbdatabase.TableChange{
		Table: table,
		PrimaryKey: &pbdatabase.TableChange_Pk{
			Pk: pk,
		},
		Operation: pbdatabase.TableChange_OPERATION_CREATE,
		Fields:    getFields(fieldsAndValues...),
	}
}

func insertRowMultiplePK(table string, pk map[string]string, fieldsAndValues ...string) *pbdatabase.TableChange {
	return &pbdatabase.TableChange{
		Table: table,
		PrimaryKey: &pbdatabase.TableChange_CompositePk{
			CompositePk: &pbdatabase.CompositePrimaryKey{
				Keys: pk,
			},
		},
		Operation: pbdatabase.TableChange_OPERATION_CREATE,
		Fields:    getFields(fieldsAndValues...),
	}
}

func upsertRowSinglePK(table string, pk string, fieldsAndValues ...string) *pbdatabase.TableChange {
	return &pbdatabase.TableChange{
		Table: table,
		PrimaryKey: &pbdatabase.TableChange_Pk{
			Pk: pk,
		},
		Operation: pbdatabase.TableChange_OPERATION_UPSERT,
		Fields:    getFields(fieldsAndValues...),
	}
}

func upsertRowMultiplePK(table string, pk map[string]string, fieldsAndValues ...string) *pbdatabase.TableChange {
	return &pbdatabase.TableChange{
		Table: table,
		PrimaryKey: &pbdatabase.TableChange_CompositePk{
			CompositePk: &pbdatabase.CompositePrimaryKey{
				Keys: pk,
			},
		},
		Operation: pbdatabase.TableChange_OPERATION_UPSERT,
		Fields:    getFields(fieldsAndValues...),
	}
}

func updateRowMultiplePK(table string, pk map[string]string, fieldsAndValues ...string) *pbdatabase.TableChange {
	return &pbdatabase.TableChange{
		Table: table,
		PrimaryKey: &pbdatabase.TableChange_CompositePk{
			CompositePk: &pbdatabase.CompositePrimaryKey{
				Keys: pk,
			},
		},
		Operation: pbdatabase.TableChange_OPERATION_UPDATE,
		Fields:    getFields(fieldsAndValues...),
	}
}
func deleteRowMultiplePK(table string, pk map[string]string) *pbdatabase.TableChange {
	return &pbdatabase.TableChange{
		Table: table,
		PrimaryKey: &pbdatabase.TableChange_CompositePk{
			CompositePk: &pbdatabase.CompositePrimaryKey{
				Keys: pk,
			},
		},
		Operation: pbdatabase.TableChange_OPERATION_DELETE,
	}
}

func dbChangesBlockData(t *testing.T, blockIdentifier string, lib finalBlock, changes ...*pbdatabase.TableChange) *pbsubstreamsrpc.Response {
	t.Helper()

	return blockScopedData(t, blockIdentifier, &pbdatabase.DatabaseChanges{TableChanges: changes}, lib)
}

func mustNewTableInfo(schema, name string, pkList []string, columnsByName map[string]*db2.ColumnInfo) *db2.TableInfo {
	ti, err := db2.NewTableInfo(schema, name, pkList, columnsByName)
	if err != nil {
		panic(err)
	}
	return ti
}

func clock(id string, num uint64) *pbsubstreams.Clock {
	return &pbsubstreams.Clock{Id: id, Number: num}
}

func simpleCursor(num, finalNum uint64) string {
	id := fmt.Sprintf("%d", num)
	finalID := fmt.Sprintf("%d", finalNum)
	blk := bstream.NewBlockRef(id, num)
	lib := bstream.NewBlockRef(finalID, finalNum)
	step := bstream.StepNew
	if id == finalID {
		step = bstream.StepNewIrreversible
	}

	return (&bstream.Cursor{
		Step:      step,
		Block:     blk,
		LIB:       lib,
		HeadBlock: blk,
	}).ToOpaque()
}

type sinkerSetupInput interface {
	ToSQL() string
}

type tablesInput map[string]*db2.TableInfo

func (t tablesInput) ToSQL() string {
	return db2.GenerateCreateTableSQL(t)
}

type rawSQLInput string

func (r rawSQLInput) ToSQL() string {
	return string(r)
}
