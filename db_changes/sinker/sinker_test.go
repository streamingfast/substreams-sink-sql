package sinker

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	pbdatabase "github.com/streamingfast/substreams-sink-database-changes/pb/sf/substreams/sink/database/v1"
	db2 "github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams/client"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

var logger *zap.Logger
var tracer logging.Tracer

func init() {
	logger, tracer = logging.ApplicationLogger("test", "test")
}

func TestSinker_SQLStatements(t *testing.T) {
	tests := []struct {
		name      string
		events    []event
		expectSQL []string
	}{
		{
			name: "insert final block",
			events: []event{
				{
					blockNum:     10,
					libNum:       10,
					tableChanges: []*pbdatabase.TableChange{insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1")},
				},
			},
			expectSQL: []string{
				`INSERT INTO "testschema"."xfer" ("from","id","to") VALUES ('sender1','1234','receiver1');`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 10;`,
				`UPDATE "testschema"."cursors" set cursor = 'bN7dsAhRyo44yl_ykkjA36WwLpc_DFtvXwrlIBBBj4r2', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
			},
		},
		{
			name: "insert two final blocks",
			events: []event{
				{
					blockNum:     10,
					libNum:       10,
					tableChanges: []*pbdatabase.TableChange{insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1")},
				},
				{
					blockNum:     11,
					libNum:       11,
					tableChanges: []*pbdatabase.TableChange{insertRowSinglePK("xfer", "2345", "from", "sender2", "to", "receiver2")},
				},
			},
			expectSQL: []string{
				`INSERT INTO "testschema"."xfer" ("from","id","to") VALUES ('sender1','1234','receiver1');`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 10;`,
				`UPDATE "testschema"."cursors" set cursor = 'bN7dsAhRyo44yl_ykkjA36WwLpc_DFtvXwrlIBBBj4r2', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
				`INSERT INTO "testschema"."xfer" ("from","id","to") VALUES ('sender2','2345','receiver2');`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 11;`,
				`UPDATE "testschema"."cursors" set cursor = 'dR5-m-1v1TQvlVRfIM9SXaWwLpc_DFtuXwrkIBBAj4r3', block_num = 11, block_id = '11' WHERE id = '756e75736564';`,
				`COMMIT`,
			},
		},
		{
			name: "insert a reversible blocks",
			events: []event{
				{
					blockNum:     10,
					libNum:       5,
					tableChanges: []*pbdatabase.TableChange{insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1")},
				},
			},
			expectSQL: []string{
				`INSERT INTO "testschema"."substreams_history" (op,table_name,pk,block_num) values ('I','"testschema"."xfer"','{"id":"1234"}',10);` +
					`INSERT INTO "testschema"."xfer" ("from","id","to") VALUES ('sender1','1234','receiver1');`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 5;`,
				`UPDATE "testschema"."cursors" set cursor = 'i4tY9gOcWnhKoGjRCl2VUKWwLpcyB1plVAvvLxtE', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
			},
		},
		{
			name: "insert, then update",
			events: []event{
				{
					blockNum:     10,
					libNum:       5,
					tableChanges: []*pbdatabase.TableChange{insertRowMultiplePK("xfer", map[string]string{"id": "1234", "idx": "3"}, "from", "sender1", "to", "receiver1")},
				},
				{
					blockNum: 11,
					libNum:   6,
					tableChanges: []*pbdatabase.TableChange{
						updateRowMultiplePK("xfer", map[string]string{"id": "2345", "idx": "3"}, "from", "sender2", "to", "receiver2"),
					},
				},
			},
			expectSQL: []string{
				`INSERT INTO "testschema"."substreams_history" (op,table_name,pk,block_num) values ('I','"testschema"."xfer"','{"id":"1234","idx":"3"}',10);` +
					`INSERT INTO "testschema"."xfer" ("from","id","to") VALUES ('sender1','1234','receiver1');`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 5;`,
				`UPDATE "testschema"."cursors" set cursor = 'i4tY9gOcWnhKoGjRCl2VUKWwLpcyB1plVAvvLxtE', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
				`INSERT INTO "testschema"."substreams_history" (op,table_name,pk,prev_value,block_num) SELECT 'U','"testschema"."xfer"','{"id":"2345","idx":"3"}',row_to_json("xfer"),11 FROM "testschema"."xfer" WHERE "id" = '2345' AND "idx" = '3';` +
					`UPDATE "testschema"."xfer" SET "from"='sender2', "to"='receiver2' WHERE "id" = '2345' AND "idx" = '3'`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 6;`,
				`UPDATE "testschema"."cursors" set cursor = 'LamYQ1PoEJyzLTRd7kdEiKWwLpcyB1tlVArvLBtH', block_num = 11, block_id = '11' WHERE id = '756e75736564';`,
				`COMMIT`,
			},
		},

		{
			name: "insert, then update, then delete (update disappears)",
			events: []event{
				{
					blockNum:     10,
					libNum:       5,
					tableChanges: []*pbdatabase.TableChange{insertRowMultiplePK("xfer", map[string]string{"id": "1234", "idx": "3"}, "from", "sender1", "to", "receiver1")},
				},
				{
					blockNum: 11,
					libNum:   6,
					tableChanges: []*pbdatabase.TableChange{
						updateRowMultiplePK("xfer", map[string]string{"id": "2345", "idx": "3"}, "from", "sender2", "to", "receiver2"),
						deleteRowMultiplePK("xfer", map[string]string{"id": "2345", "idx": "3"}),
					},
				},
			},
			expectSQL: []string{
				`INSERT INTO "testschema"."substreams_history" (op,table_name,pk,block_num) values ('I','"testschema"."xfer"','{"id":"1234","idx":"3"}',10);` +
					`INSERT INTO "testschema"."xfer" ("from","id","to") VALUES ('sender1','1234','receiver1');`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 5;`,
				`UPDATE "testschema"."cursors" set cursor = 'i4tY9gOcWnhKoGjRCl2VUKWwLpcyB1plVAvvLxtE', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
				// the following gets deduped
				//`INSERT INTO "testschema"."substreams_history" (op,table_name,pk,prev_value,block_num) SELECT 'U','"testschema"."xfer"','{"id":"2345","idx":"3"}',row_to_json("xfer"),11 FROM "testschema"."xfer" WHERE "id" = '2345' AND "idx" = '3';` +
				//	`UPDATE "testschema"."xfer" SET "from"='sender2', "to"='receiver2' WHERE "id" = '2345' AND "idx" = '3'`,
				`INSERT INTO "testschema"."substreams_history" (op,table_name,pk,prev_value,block_num) SELECT 'D','"testschema"."xfer"','{"id":"2345","idx":"3"}',row_to_json("xfer"),11 FROM "testschema"."xfer" WHERE "id" = '2345' AND "idx" = '3';` +
					`DELETE FROM "testschema"."xfer" WHERE "id" = '2345' AND "idx" = '3'`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 6;`,
				`UPDATE "testschema"."cursors" set cursor = 'LamYQ1PoEJyzLTRd7kdEiKWwLpcyB1tlVArvLBtH', block_num = 11, block_id = '11' WHERE id = '756e75736564';`,
				`COMMIT`,
			},
		},

		{
			name: "insert two reversible blocks, then UNDO last",
			events: []event{
				{
					blockNum:     10,
					libNum:       5,
					tableChanges: []*pbdatabase.TableChange{insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1")},
				},
				{
					blockNum:     11,
					libNum:       5,
					tableChanges: []*pbdatabase.TableChange{insertRowSinglePK("xfer", "2345", "from", "sender2", "to", "receiver2")},
				},
				{
					blockNum:   10, // undo everything above 10
					libNum:     5,
					undoSignal: true,
				},
			},
			expectSQL: []string{
				`INSERT INTO "testschema"."substreams_history" (op,table_name,pk,block_num) values ('I','"testschema"."xfer"','{"id":"1234"}',10);` +
					`INSERT INTO "testschema"."xfer" ("from","id","to") VALUES ('sender1','1234','receiver1');`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 5;`,
				`UPDATE "testschema"."cursors" set cursor = 'i4tY9gOcWnhKoGjRCl2VUKWwLpcyB1plVAvvLxtE', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
				`INSERT INTO "testschema"."substreams_history" (op,table_name,pk,block_num) values ('I','"testschema"."xfer"','{"id":"2345"}',11);` +
					`INSERT INTO "testschema"."xfer" ("from","id","to") VALUES ('sender2','2345','receiver2');`,
				`DELETE FROM "testschema"."substreams_history" WHERE block_num <= 5;`,
				`UPDATE "testschema"."cursors" set cursor = 'Euaqz6R-ylLG0gbdej7Me6WwLpcyB1tlVArvLxtE', block_num = 11, block_id = '11' WHERE id = '756e75736564';`,
				`COMMIT`,
				`SELECT op,table_name,pk,prev_value,block_num FROM "testschema"."substreams_history" WHERE "block_num" > 10 ORDER BY "block_num" DESC`,

				//`DELETE FROM "testschema"."xfer" WHERE "id" = "2345";`, // this mechanism is tested in db.revertOp
				`DELETE FROM "testschema"."substreams_history" WHERE "block_num" > 10;`,
				`UPDATE "testschema"."cursors" set cursor = 'i4tY9gOcWnhKoGjRCl2VUKWwLpcyB1plVAvvLxtE', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			tx := &db2.TestTx{}
			l := db2.NewTestLoader(
				t,
				"psql://x:5432/x?schemaName=testschema",
				"testschema",
				tx,
				db2.TestTables("testschema"),
				logger,
				tracer,
			)
			s, err := sink.New(sink.SubstreamsModeDevelopment, false, testPackage, testPackage.Modules.Modules[0], []byte("unused"), testClientConfig, logger, nil)
			require.NoError(t, err)
			sinker, _ := New(s, l, logger, nil)

			for _, evt := range test.events {
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

			results := tx.Results()
			assert.Equal(t, test.expectSQL, results)
		})
	}
}

func TestSinker_Integration(t *testing.T) {
	ctx := context.Background()
	dbConnectionString, postgresContainer := setupPostgresContainer(t)

	tests := []struct {
		name                   string
		events                 []event
		expectedQueryResponses []*XferRow
		expectedFinalCursor    string
	}{
		{
			"insert final block",
			[]event{
				newEvent(10, 10,
					insertRowSinglePK("xfer", "1234", "from", "sender1", "to", "receiver1"),
				),
			},
			[]*XferRow{
				{ID: "1234", From: "sender1", To: "receiver1"},
			},
			"Block #10 (10) - LIB #10 (10)",
		},
		{
			"insert block, then undo insertion",
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
			"upsert single pk, final",
			[]event{
				newEvent(10, 10,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
			},
			[]*XferRow{
				{ID: "1234", From: "sender2", To: "receiver2"},
			},
			"Block #10 (10) - LIB #10 (10)",
		},
		{
			"upsert single pk, first insert, second update, final",
			[]event{
				newEvent(10, 10,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				newEvent(11, 11,
					upsertRowSinglePK("xfer", "1234", "to", "receiver3"),
				),
			},
			[]*XferRow{
				{ID: "1234", From: "sender2", To: "receiver3"},
			},
			"Block #11 (11) - LIB #11 (11)",
		},

		{
			"upsert single pk, undo insertion",
			[]event{
				newEvent(10, 8,
					upsertRowSinglePK("xfer", "1234", "from", "sender2", "to", "receiver2"),
				),
				newUndoEvent(9, 8),
			},
			nil,
			"Block #9 (9) - LIB #8 (8)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(func() {
				require.NoError(t, postgresContainer.Restore(ctx))
			})

			ctx := context.Background()
			l := db2.NewTestLoader(
				t,
				dbConnectionString+"&schemaName=testschema",
				"testschema",
				nil,
				db2.TestTables("testschema"),
				logger,
				tracer,
			)

			s, err := sink.New(sink.SubstreamsModeDevelopment, false, testPackage, testPackage.Modules.Modules[0], []byte("unused"), testClientConfig, logger, nil)
			require.NoError(t, err)
			sinker, _ := New(s, l, logger, nil)
			t.Cleanup(func() { sinker.loader.Close() })

			require.NoError(t, l.InsertCursor(ctx, sinker.OutputModuleHash(), sink.NewBlankCursor()))

			for _, evt := range test.events {
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

			rows, err := l.QueryContext(ctx, `SELECT id, "from", "to" FROM "testschema"."xfer"`)
			require.NoError(t, err)
			t.Cleanup(func() { rows.Close() })

			xferRows := readXferRows(t, l.DB)

			require.NoError(t, rows.Err())
			require.Equal(t, test.expectedQueryResponses, xferRows)

			finalCursor, mistmatchDetected, err := l.GetCursor(ctx, sinker.OutputModuleHash())
			require.NoError(t, err)
			require.False(t, mistmatchDetected)

			actualCursor := fmt.Sprintf("Block %s - LIB %s", finalCursor.Block(), finalCursor.LIB)
			require.Equal(t, test.expectedFinalCursor, actualCursor)
		})
	}
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

func readXferRows(t *testing.T, db *sql.DB) (rows []*XferRow) {
	t.Helper()

	rawRows, err := db.QueryContext(context.Background(), `SELECT id, "from", "to" FROM "testschema"."xfer"`)
	require.NoError(t, err)
	defer rawRows.Close()

	for rawRows.Next() {
		var row XferRow
		require.NoError(t, rawRows.Scan(&row.ID, &row.From, &row.To))

		rows = append(rows, &row)
	}
	require.NoError(t, rawRows.Err())

	return rows
}

func setupPostgresContainer(t *testing.T) (dbConnectionString string, container *postgres.PostgresContainer) {
	t.Helper()
	ctx := context.Background()

	dbName := "users"
	dbUser := "user"
	dbPassword := "password"

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	testcontainers.CleanupContainer(t, postgresContainer)
	require.NoError(t, err)

	dbConnectionString, err = postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	sqls := []string{
		`CREATE SCHEMA testschema;`,
	}

	_, _, err = postgresContainer.Exec(ctx, []string{"psql", "-U", dbUser, "-d", dbName, "-c", strings.Join(sqls, "\n")})
	require.NoError(t, err)

	testTables := db2.TestTables("testschema")

	l := db2.NewTestLoader(
		t,
		dbConnectionString+"&schemaName=testschema",
		"testschema",
		nil,
		testTables,
		logger,
		tracer,
	)

	err = l.Setup(context.Background(), "testschema", db2.GenerateCreateTableSQL(testTables), false)
	require.NoError(t, err)

	require.NoError(t, l.Close())

	err = postgresContainer.Snapshot(ctx)
	require.NoError(t, err)

	return dbConnectionString, postgresContainer
}

type XferRow struct {
	ID   string `db:"id"`
	From string `db:"from"`
	To   string `db:"to"`
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

func pruneAbove(blockNum uint64) string {
	return fmt.Sprintf(`DELETE FROM "testschema"."inserts_history" WHERE block_num > %d;DELETE FROM "testschema"."updates_history" WHERE block_num > %d;DELETE FROM "testschema"."deletes_history" WHERE block_num > %d;`,
		blockNum, blockNum, blockNum)
}

func pruneBelow(blockNum uint64) string {
	return fmt.Sprintf(`DELETE FROM "testschema"."inserts_history" WHERE block_num <= %d;DELETE FROM "testschema"."updates_history" WHERE block_num <= %d;DELETE FROM "testschema"."deletes_history" WHERE block_num <= %d;`,
		blockNum, blockNum, blockNum)
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
