package db

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
)

var ErrCursorNotFound = errors.New("cursor not found")

func (l *Loader) GetCursor(outputModuleHash string) (*sink.Cursor, error) {
	type cursorRow struct {
		Id       string
		Cursor   string
		BlockNum uint64
		BlockID  string
	}

	query := fmt.Sprintf("SELECT id, cursor, block_num, block_id from cursors WHERE id = '%s'", outputModuleHash)
	row := l.DB.QueryRow(query)

	c := &cursorRow{}
	if err := row.Scan(&c.Id, &c.Cursor, &c.BlockNum, &c.BlockID); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrCursorNotFound
		}
		return nil, fmt.Errorf("getting cursor %q:  %w", outputModuleHash, err)
	}

	return sink.NewCursor(c.Cursor, bstream.NewBlockRef(c.BlockID, c.BlockNum)), nil
}

func (l *Loader) WriteCursor(moduleHash string, c *sink.Cursor) error {
	query := fmt.Sprintf("INSERT INTO cursors (id, cursor, block_num, block_id) values ('%s', '%s', %d, '%s')", moduleHash, c.Cursor, c.Block.Num(), c.Block.ID())
	if _, err := l.DB.Exec(query); err != nil {
		return fmt.Errorf("write cursor: %w", err)
	}
	return nil
}

func (l *Loader) UpdateCursorQuery(moduleHash string, c *sink.Cursor) string {
	return fmt.Sprintf("UPDATE cursors set cursor = '%s', block_num = %d, block_id = '%s' WHERE id = '%s'", c.Cursor, c.Block.Num(), c.Block.ID(), moduleHash)
}
