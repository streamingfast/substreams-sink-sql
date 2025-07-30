package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
	pbdatabase "github.com/streamingfast/substreams-sink-database-changes/pb/sf/substreams/sink/database/v1"
	db2 "github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams-sink-sql/db_changes/sinker"
	"github.com/streamingfast/substreams/client"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestSinker_Integration_SinglePrimaryKey(t *testing.T) {
	testTables := db2.TestSinglePrimaryKeyTables("testschema")
	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t, testTables, nil)

	type XferSinglePKRow struct {
		ID   string `db:"id"`
		From string `db:"from"`
		To   string `db:"to"`
	}

	tests := []struct {
		name                   string
		events                 []event
		expectedQueryResponses []*XferSinglePKRow
		expectedFinalCursor    string
	}{
		{
			"insert final",
			[]event{
				newEvent(10, 10,
					insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1"),
				),
			},
			[]*XferSinglePKRow{
				{ID: "1234", From: "sender1", To: "receiver1"},
			},
			"Block #10 (10) - LIB #10 (10)",
		},
		{
			"insert then undo insertion",
			[]event{
				newEvent(10, 8,
					insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1"),
				),
				newUndoEvent(9, 8),
			},
			nil,
			"Block #9 (9) - LIB #8 (8)",
		},

		{
			"upsert final",
			[]event{
				newEvent(10, 10,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
			},
			[]*XferSinglePKRow{
				{ID: "1234", From: "sender2", To: "receiver2"},
			},
			"Block #10 (10) - LIB #10 (10)",
		},
		{
			"upsert, first insert, second update, final",
			[]event{
				newEvent(10, 10,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 11,
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
			},
			[]*XferSinglePKRow{
				{ID: "1234", From: "sender2", To: "receiver3"},
			},
			"Block #11 (11) - LIB #11 (11)",
		},

		{
			"upsert, first insert, undo insertion",
			[]event{
				newEvent(10, 8,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				newUndoEvent(9, 8),
			},
			nil,
			"Block #9 (9) - LIB #8 (8)",
		},
		{
			"upsert, first insert, second update, undo initial insert",
			[]event{
				newEvent(10, 8,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 8,
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
				newUndoEvent(9, 8),
			},
			nil,
			"Block #9 (9) - LIB #8 (8)",
		},
		{
			"upsert, first insert, second update, undo last update",
			[]event{
				newEvent(10, 8,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 8,
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
				newUndoEvent(10, 8),
			},
			[]*XferSinglePKRow{
				{ID: "1234", From: "sender2", To: "receiver2"},
			},
			"Block #10 (10) - LIB #8 (8)",
		},
		{
			"upsert, first insert, second update, third update, undo last update",
			[]event{
				newEvent(10, 8,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 8,
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
				newEvent(12, 8,
					upsertRowSinglePK("xfer", "1234", "from", "sender3"),
				),
				newUndoEvent(11, 8),
			},
			[]*XferSinglePKRow{
				{ID: "1234", From: "sender2", To: "receiver3"},
			},
			"Block #11 (11) - LIB #8 (8)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runSinkerTest(
				t,
				testTables,
				dbConnectionString,
				postgresContainer,
				test.events,
				test.expectedQueryResponses,
				test.expectedFinalCursor,
			)
		})
	}
}

func TestSinker_Integration_CompositePrimaryKey(t *testing.T) {
	testTables := db2.TestTables("testschema", map[string]*db2.TableInfo{
		"xfer": mustNewTableInfo("testschema", "xfer", []string{"id", "number"}, map[string]*db2.ColumnInfo{
			"id":     db2.NewColumnInfo("id", "text", ""),
			"number": db2.NewColumnInfo("number", "bigint", ""),
			"from":   db2.NewColumnInfo("from", "text", ""),
			"to":     db2.NewColumnInfo("to", "text", ""),
		}),
	})

	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t, testTables, nil)

	pk := compositePK

	type XferCompositePKRow struct {
		ID     string `db:"id"`
		Number string `db:"number"`
		From   string `db:"from"`
		To     string `db:"to"`
	}

	tests := []struct {
		name                   string
		events                 []event
		expectedQueryResponses []*XferCompositePKRow
		expectedFinalCursor    string
	}{
		{
			"insert final",
			[]event{
				newEvent(10, 10,
					insertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender1", "to", "receiver1"),
				),
			},
			[]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender1", To: "receiver1"},
			},
			"Block #10 (10) - LIB #10 (10)",
		},
		{
			"insert then undo insertion",
			[]event{
				newEvent(10, 8,
					insertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender1", "to", "receiver1"),
				),
				newUndoEvent(9, 8),
			},
			nil,
			"Block #9 (9) - LIB #8 (8)",
		},

		{
			"upsert final",
			[]event{
				newEvent(10, 10,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
			},
			[]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender2", To: "receiver2"},
			},
			"Block #10 (10) - LIB #10 (10)",
		},
		{
			"upsert, first insert, second update, final",
			[]event{
				newEvent(10, 10,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 11,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver3"),
				),
			},
			[]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender2", To: "receiver3"},
			},
			"Block #11 (11) - LIB #11 (11)",
		},

		{
			"upsert, first insert, undo insertion",
			[]event{
				newEvent(10, 8,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				newUndoEvent(9, 8),
			},
			nil,
			"Block #9 (9) - LIB #8 (8)",
		},
		{
			"upsert, first insert, second update, undo initial insert",
			[]event{
				newEvent(10, 8,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 8,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver3"),
				),
				newUndoEvent(9, 8),
			},
			nil,
			"Block #9 (9) - LIB #8 (8)",
		},
		{
			"upsert, first insert, second update, undo last update",
			[]event{
				newEvent(10, 8,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 8,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver3"),
				),
				newUndoEvent(10, 8),
			},
			[]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender2", To: "receiver2"},
			},
			"Block #10 (10) - LIB #8 (8)",
		},
		{
			"upsert, first insert, second update, third update, undo last update",
			[]event{
				newEvent(10, 8,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 8,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "to", "receiver3"),
				),
				newEvent(12, 8,
					upsertRowMultiplePK("xfer", pk("id", "12", "number", "34"), "from", "sender3"),
				),
				newUndoEvent(11, 8),
			},
			[]*XferCompositePKRow{
				{ID: "12", Number: "34", From: "sender2", To: "receiver3"},
			},
			"Block #11 (11) - LIB #8 (8)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runSinkerTest(
				t,
				testTables,
				dbConnectionString,
				postgresContainer,
				test.events,
				test.expectedQueryResponses,
				test.expectedFinalCursor,
			)
		})
	}
}

func TestSinker_Integration_Bytes(t *testing.T) {
	schema := "testschema"
	testTables := db2.TestTables(schema, map[string]*db2.TableInfo{
		"xfer": mustNewTableInfo(schema, "xfer", []string{"id"}, map[string]*db2.ColumnInfo{
			"id":    db2.NewColumnInfo("id", "bytea", []byte{}),
			"value": db2.NewColumnInfo("value", "bytea", []byte{}),
		}),
	})

	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t, testTables, nil)

	type XferBytesRow struct {
		ID    []byte `db:"id"`
		Value []byte `db:"value"`
	}

	runSinkerTest(
		t,
		testTables,
		dbConnectionString,
		postgresContainer,
		[]event{
			newEvent(10, 10,
				insertRowSinglePK("xfer", `\x01`, "value", `\x04ab`),
			),
		},
		[]*XferBytesRow{
			{ID: []byte{0x01}, Value: []byte{0x04, 0xab}},
		},
		"Block #10 (10) - LIB #10 (10)",
	)
}

func TestSinker_Integration_ParentChildOrdering(t *testing.T) {
	schema := "testschema"
	testTables := db2.TestTables(schema, map[string]*db2.TableInfo{
		"xfer": mustNewTableInfo(schema, "xfer", []string{"id"}, map[string]*db2.ColumnInfo{
			"id":   db2.NewColumnInfo("id", "text", ""),
			"from": db2.NewColumnInfo("from", "text", ""),
		}),

		"users": mustNewTableInfo(schema, "users", []string{"id"}, map[string]*db2.ColumnInfo{
			"id": db2.NewColumnInfo("id", "text", ""),
		}),
	})

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
	`, schema, schema, schema)

	dbConnectionString, postgresContainer := setupDbChangesPostgresContainer(t, testTables, &sqlSchema)

	type XferRow struct {
		ID   string `db:"id"`
		From string `db:"from"`
	}

	type UserRow struct {
		ID string `db:"id"`
	}

	runSinkerTest(
		t,
		testTables,
		dbConnectionString,
		postgresContainer,
		[]event{
			newEvent(10, 10,
				insertRowSinglePK("users", "user1"),
				insertRowSinglePK("xfer", "xfer1", "from", "user1"),
			),
		},
		[]*XferRow{
			{ID: "xfer1", From: "user1"},
		},
		"Block #10 (10) - LIB #10 (10)",
	)
}

func runSinkerTest[R any](
	t *testing.T,
	tables map[string]*db2.TableInfo,
	dbDSN string,
	postgresContainer *postgres.PostgresContainer,
	events []event,
	expectedQueryResponses []R,
	expectedFinalCursor string,
) {
	t.Helper()

	ctx := context.Background()
	t.Cleanup(func() {
		require.NoError(t, postgresContainer.Restore(ctx))
	})

	l := db2.NewTestLoader(
		t,
		dbDSN,
		nil,
		tables,
		logger,
		tracer,
	)

	s, err := sink.New(sink.SubstreamsModeDevelopment, false, testPackage, testPackage.Modules.Modules[0], []byte("unused"), testClientConfig, logger, nil)
	require.NoError(t, err)
	sinker, _ := sinker.New(s, l, logger, nil, 3, 1*time.Second)
	t.Cleanup(func() { sinker.Close() })

	require.NoError(t, l.InsertCursor(ctx, sinker.OutputModuleHash(), sink.NewBlankCursor()))

	for _, evt := range events {
		if evt.undoSignal {
			cursor := simpleCursor(evt.blockNum, evt.libNum)
			err := sinker.HandleBlockUndoSignal(ctx, &pbsubstreamsrpc.BlockUndoSignal{
				LastValidBlock:  &pbsubstreams.BlockRef{Id: fmt.Sprintf("%d", evt.blockNum), Number: evt.blockNum},
				LastValidCursor: cursor,
			}, sink.MustNewCursor(cursor))
			require.NoError(t, err)
			continue
		}

		err := sinker.HandleBlockScopedData(
			ctx,
			blockScopedData("db_out", evt.tableChanges, evt.blockNum, evt.libNum),
			flushEveryBlock, sink.MustNewCursor(simpleCursor(evt.blockNum, evt.libNum)),
		)
		require.NoError(t, err)
	}

	dbx := sqlx.NewDb(l.DB, "postgres")

	var rows []R
	readQuery := fmt.Sprintf(`SELECT * FROM "%s"."xfer"`, l.GetDSN().Schema())

	err = dbx.SelectContext(context.Background(), &rows, readQuery)
	require.NoError(t, err)

	require.Equal(t, expectedQueryResponses, rows)

	finalCursor, mismatchDetected, err := l.GetCursor(ctx, sinker.OutputModuleHash())
	require.NoError(t, err)
	require.False(t, mismatchDetected)

	actualCursor := fmt.Sprintf("Block %s - LIB %s", finalCursor.Block(), finalCursor.LIB)
	require.Equal(t, expectedFinalCursor, actualCursor)
}

type event struct {
	blockNum     uint64
	libNum       uint64
	tableChanges []*pbdatabase.TableChange
	undoSignal   bool
}

func newEvent(blockNum, libNum uint64, tableChanges ...*pbdatabase.TableChange) event {
	return event{
		blockNum:     blockNum,
		libNum:       libNum,
		tableChanges: tableChanges,
		undoSignal:   false,
	}
}

func newUndoEvent(blockNum, libNum uint64) event {
	return event{
		blockNum:   blockNum,
		libNum:     libNum,
		undoSignal: true,
	}
}

var T = true
var flushEveryBlock = &T

var testPackage = &pbsubstreams.Package{
	Modules: &pbsubstreams.Modules{
		Modules: []*pbsubstreams.Module{
			{
				Name: "db_out",
				Kind: &pbsubstreams.Module_KindMap_{},
				Output: &pbsubstreams.Module_Output{
					Type: "proto:sf.substreams.sink.database.v1.DatabaseChanges",
				},
			},
		},
	},
}

var testClientConfig = &client.SubstreamsClientConfig{}

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

func blockScopedData(module string, changes []*pbdatabase.TableChange, blockNum uint64, finalBlockNum uint64) *pbsubstreamsrpc.BlockScopedData {
	mapOutput, err := anypb.New(&pbdatabase.DatabaseChanges{
		TableChanges: changes,
	})
	if err != nil {
		panic(err)
	}

	return &pbsubstreamsrpc.BlockScopedData{
		Output: &pbsubstreamsrpc.MapModuleOutput{
			Name:      module,
			MapOutput: mapOutput,
		},
		Clock:            clock(fmt.Sprintf("%d", blockNum), blockNum),
		Cursor:           simpleCursor(blockNum, finalBlockNum),
		FinalBlockHeight: finalBlockNum,
	}
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

func ptr[T any](v T) *T {
	return &v
}
