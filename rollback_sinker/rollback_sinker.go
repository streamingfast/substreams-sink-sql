package rollback_sinker

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

type RollbackSinker struct {
	*sinker.PostgresSinker
	rollbackUrl    string
	rollbackSchema string
	logger         *zap.Logger
}

func New(postgresSinker *sinker.PostgresSinker, rollbackUrl string, rollbackSchema string, zlog *zap.Logger) (*RollbackSinker, error) {
	return &RollbackSinker{
		PostgresSinker: postgresSinker,
		rollbackUrl:    rollbackUrl,
		rollbackSchema: rollbackSchema,
		logger:         zlog,
	}, nil
}

func asBlockRef(blockRef *pbsubstreams.BlockRef) bstream.BlockRef {
	return bstream.NewBlockRef(blockRef.Id, blockRef.Number)
}

func (s *RollbackSinker) HandleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	if (s.rollbackUrl == "") || (s.rollbackSchema == "") {
		s.logger.Info("rollback url or schema not set, skipping...")
		return s.PostgresSinker.HandleBlockUndoSignal(ctx, data, cursor)
	}

	s.logger.Info("Performing rollback...")

	lastValidBlock := asBlockRef(data.LastValidBlock)
	lastValidBlock.Num()
	params := url.Values{}
	params.Add("last_valid_block", strconv.FormatUint(lastValidBlock.Num(), 10))
	params.Add("schema", s.rollbackSchema)
	u, _ := url.ParseRequestURI(s.rollbackUrl)
	u.RawQuery = params.Encode()
	urlStr := fmt.Sprintf("%v", u)
	resp, err := http.Get(urlStr)
	if err != nil {
		return fmt.Errorf("Rollback failed")
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return fmt.Errorf("Error reading response body")
	}

	resp.Body.Close()

	s.logger.Info(string(body))

	return nil
}
