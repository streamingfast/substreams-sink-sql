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
)

type RollbackSinker struct {
	*sinker.PostgresSinker
	rollbackUrl    string
	rollbackSchema string
}

func New(postgresSinker *sinker.PostgresSinker, rollbackUrl string, rollbackSchema string) (*RollbackSinker, error) {
	return &RollbackSinker{
		PostgresSinker: postgresSinker,
		rollbackUrl:    rollbackUrl,
		rollbackSchema: rollbackSchema,
	}, nil
}

func asBlockRef(blockRef *pbsubstreams.BlockRef) bstream.BlockRef {
	return bstream.NewBlockRef(blockRef.Id, blockRef.Number)
}

func (s *RollbackSinker) HandleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	if (s.rollbackUrl == "") || (s.rollbackSchema == "") {
		return s.PostgresSinker.HandleBlockUndoSignal(ctx, data, cursor)
	}

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

	fmt.Println(string(body))

	return nil
}
