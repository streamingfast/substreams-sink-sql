package state

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
	"gopkg.in/yaml.v3"
)

var _ Store = (*FileStateStore)(nil)

type FileStateStore struct {
	startOnce sync.Once

	outputPath string

	state *FileState
}

func NewFileStateStore(outputPath string) (*FileStateStore, error) {
	s := &FileState{}
	content, err := os.ReadFile(outputPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if err != nil && os.IsNotExist(err) {
		s = newFileState()
	}

	if err := yaml.Unmarshal(content, s); err != nil {
		return nil, fmt.Errorf("unmarshal state file %q: %w", outputPath, err)
	}
	return &FileStateStore{
		outputPath: outputPath,
		state:      s,
	}, nil
}

func (s *FileStateStore) ReadCursor() (cursor *sink.Cursor, err error) {
	return sink.NewCursor(s.state.Cursor)
}

func (s *FileStateStore) NewBoundary(boundary *bstream.Range) {
	s.state.ActiveBoundary.StartBlockNumber = boundary.StartBlock()
	s.state.ActiveBoundary.EndBlockNumber = *boundary.EndBlock()
}

func (s *FileStateStore) SetCursor(cursor *sink.Cursor) {
	s.startOnce.Do(func() {
		restartAt := time.Now()
		if s.state.StartedAt.IsZero() {
			s.state.StartedAt = restartAt
		}
		s.state.RestartedAt = restartAt
	})

	s.state.Cursor = cursor.String()
	s.state.Block = BlockState{
		ID:     cursor.Block().ID(),
		Number: cursor.Block().Num(),
	}
}

func (s *FileStateStore) GetState() (Saveable, error) {
	cnt, err := yaml.Marshal(s.state)
	if err != nil {
		return nil, fmt.Errorf("marshall: %w", err)
	}
	return &stateInstance{
		data: cnt,
		path: s.outputPath,
	}, nil
}

type FileState struct {
	Cursor         string         `yaml:"cursor" json:"cursor"`
	Block          BlockState     `yaml:"block" json:"block"`
	ActiveBoundary ActiveBoundary `yaml:"active_boundary" json:"active_boundary"`

	// StartedAt is the time this process was launching initially without accounting to any restart, once set, this
	// value, it's never re-written (unless the file does not exist anymore).
	StartedAt time.Time `yaml:"started_at,omitempty" json:"started_at,omitempty"`
	// RestartedAt is the time this process was last launched meaning it's reset each time the process start. This value
	// in contrast to `StartedAt` change over time each time the process is restarted.
	RestartedAt time.Time `yaml:"restarted_at,omitempty" json:"restarted_at,omitempty"`
}

func newFileState() *FileState {
	return &FileState{
		Cursor: "",
		Block:  BlockState{"", 0},
	}
}

type BlockState struct {
	ID     string `yaml:"id" json:"id"`
	Number uint64 `yaml:"number" json:"number"`
}

type ActiveBoundary struct {
	StartBlockNumber uint64 `yaml:"start_block_number"  json:"start_block_number"`
	EndBlockNumber   uint64 `yaml:"end_block_number"  json:"end_block_number"`
}

type stateInstance struct {
	data []byte
	path string
}

func (s *stateInstance) Save() error {
	if err := os.WriteFile(s.path, s.data, os.ModePerm); err != nil {
		return fmt.Errorf("unable to write state file: %w", err)
	}
	return nil
}
