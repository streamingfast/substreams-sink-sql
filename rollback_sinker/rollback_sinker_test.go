package rollback_sinker

import (
	"context"
	"fmt"
	"testing"

	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/db"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	v1 "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/stretchr/testify/assert"
)

func TestCallRollbaack(t *testing.T) {
	t.Run("test name", func(t *testing.T) {
		var zlog, tracer = logging.RootLogger("sink-postgres", "github.com/streamingfast/substreams-sink-mongodb/cmd/substreams-sink-mongodb")
		rawSinker := new(sink.Sinker)
		dbLoader := new(db.Loader)
		postgresSinker, err := sinker.New(rawSinker, dbLoader, zlog, tracer)
		if err != nil {
			fmt.Println("unable to setup postgres sinker: %w", err)
			assert.Equal(t, true, false)
		}

		rollbackSinker, err := New(postgresSinker, "http://localhost:3000/rollback", "")
		if err != nil {
			fmt.Println("unable to setup rollback sinker: %w", err)
			assert.Equal(t, true, false)
		}
		ctx := context.TODO()
		blockRef := v1.BlockRef{
			Id:     "blcokID",
			Number: 1234,
		}
		blockUndoSignal := pbsubstreamsrpc.BlockUndoSignal{
			LastValidBlock: &blockRef,
			// Check usage of lastValidCursor
			LastValidCursor: "lastValidCursorHash",
		}

		cursor := new(sink.Cursor)
		rollbackSinker.HandleBlockUndoSignal(ctx, &blockUndoSignal, cursor)
	})
}
