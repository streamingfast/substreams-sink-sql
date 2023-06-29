package state

import (
	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
)

type Store interface {
	NewBoundary(*bstream.Range)
	ReadCursor() (*sink.Cursor, error)
	SetCursor(*sink.Cursor)
	GetState() (Saveable, error)
}

type Saveable interface {
	Save() error
}
