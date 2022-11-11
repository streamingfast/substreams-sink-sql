package db

import (
	"database/sql"
	"errors"
	"fmt"
)

var ErrCursorNotFound = errors.New("cursor not found")

type Cursor struct {
	Id       string
	Cursor   string
	BlockNum uint64
}

func (l Loader) GetCursor(outputModuleHash string) (*Cursor, error) {
	query := fmt.Sprintf("SELECT id, cursor, block_num from cursors WHERE id = '%s'", outputModuleHash)
	row := l.DB.QueryRow(query)

	c := &Cursor{}
	if err := row.Scan(&c.Id, &c.Cursor, &c.BlockNum); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrCursorNotFound
		}
		return nil, fmt.Errorf("getting curosr %q:  %w", outputModuleHash, err)
	}
	return c, nil
}

func (l Loader) WriteCursor(c *Cursor) error {
	query := fmt.Sprintf("INSERT INTO cursors (id, cursor, block_num) values ('%s', '%s', %d)", c.Id, c.Cursor, c.BlockNum)
	if _, err := l.DB.Exec(query); err != nil {
		return fmt.Errorf("write cursor: %w", err)
	}
	return nil
}

func (l Loader) updateCursorQuery(c *Cursor) string {
	return fmt.Sprintf("UPDATE cursors set cursor = '%s', block_num = %d WHERE id = '%s'", c.Cursor, c.BlockNum, c.Id)
}
