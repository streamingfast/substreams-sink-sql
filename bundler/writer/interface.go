package writer

import (
	"context"
	"github.com/streamingfast/dstore"
	"io"

	"github.com/streamingfast/bstream"
)

type Writer interface {
	io.Writer

	StartBoundary(*bstream.Range) error
	CloseBoundary(ctx context.Context) (Uploadeable, error)
	Type() FileType
}

type Uploadeable interface {
	Upload(ctx context.Context, store dstore.Store) (string, error)
}
