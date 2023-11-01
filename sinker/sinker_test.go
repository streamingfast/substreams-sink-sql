package sinker

import (
	"context"
	"fmt"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	pbdatabase "github.com/streamingfast/substreams-sink-database-changes/pb/sf/substreams/sink/database/v1"
	"github.com/streamingfast/substreams-sink-sql/db"
	"github.com/streamingfast/substreams/client"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	_ "github.com/lib/pq"
)

var T = true
var flushEveryBlock = &T

func pruneQuery(blockNum uint64) string {
	return fmt.Sprintf(`DELETE FROM "testschema"."inserts_history" WHERE block_num <= %d;DELETE FROM "testschema"."updates_history" WHERE block_num <= %d;DELETE FROM "testschema"."deletes_history" WHERE block_num <= %d;`,
		blockNum, blockNum, blockNum)
}

func TestInserts(t *testing.T) {

	logger, tracer := logging.ApplicationLogger("test", "test")

	type event struct {
		blockNum     uint64
		libNum       uint64
		tableChanges []*pbdatabase.TableChange
		// undoUpTo uint64
	}

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
				`INSERT INTO "testschema"."xfer" (from,id,to) VALUES ('sender1','1234','receiver1');`,
				pruneQuery(10),
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
				`INSERT INTO "testschema"."xfer" (from,id,to) VALUES ('sender1','1234','receiver1');`,
				pruneQuery(10),
				`UPDATE "testschema"."cursors" set cursor = 'bN7dsAhRyo44yl_ykkjA36WwLpc_DFtvXwrlIBBBj4r2', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
				`INSERT INTO "testschema"."xfer" (from,id,to) VALUES ('sender2','2345','receiver2');`,
				pruneQuery(11),
				`UPDATE "testschema"."cursors" set cursor = 'dR5-m-1v1TQvlVRfIM9SXaWwLpc_DFtuXwrkIBBAj4r3', block_num = 11, block_id = '11' WHERE id = '756e75736564';`,
				`COMMIT`,
			},
		},
		{
			name: "insert two reversible blocks",
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
			},
			expectSQL: []string{
				`INSERT INTO "testschema"."inserts_history" (table_name, id, block_num) values ('"testschema"."xfer"', '{"id":"1234"}', 10);` +
					`INSERT INTO "testschema"."xfer" (from,id,to) VALUES ('sender1','1234','receiver1');`,
				pruneQuery(5),
				`UPDATE "testschema"."cursors" set cursor = 'i4tY9gOcWnhKoGjRCl2VUKWwLpcyB1plVAvvLxtE', block_num = 10, block_id = '10' WHERE id = '756e75736564';`,
				`COMMIT`,
				`INSERT INTO "testschema"."inserts_history" (table_name, id, block_num) values ('"testschema"."xfer"', '{"id":"2345"}', 11);` +
					`INSERT INTO "testschema"."xfer" (from,id,to) VALUES ('sender2','2345','receiver2');`,
				pruneQuery(5),
				`UPDATE "testschema"."cursors" set cursor = 'Euaqz6R-ylLG0gbdej7Me6WwLpcyB1tlVArvLxtE', block_num = 11, block_id = '11' WHERE id = '756e75736564';`,
				`COMMIT`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			l, tx := db.NewTestLoader(
				logger,
				tracer,
				"testschema",
				db.TestTables("testschema"),
			)
			s, err := sink.New(sink.SubstreamsModeDevelopment, testPackage, testPackage.Modules.Modules[0], []byte("unused"), testClientConfig, logger, nil)
			require.NoError(t, err)
			sinker, _ := New(s, l, logger, nil)

			for _, evt := range test.events {
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

func insertRowSinglePK(table string, pk string, fieldsAndValues ...string) *pbdatabase.TableChange {
	return &pbdatabase.TableChange{
		Table: table,
		PrimaryKey: &pbdatabase.TableChange_Pk{
			Pk: pk,
		},
		Ordinal:   0,
		Operation: pbdatabase.TableChange_CREATE,
		Fields:    getFields(fieldsAndValues...),
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
func mustNewTableInfo(schema, name string, pkList []string, columnsByName map[string]*db.ColumnInfo) *db.TableInfo {
	ti, err := db.NewTableInfo(schema, name, pkList, columnsByName)
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
