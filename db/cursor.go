package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

var ErrCursorNotFound = errors.New("cursor not found")

// GetAllCursors returns an unordered map given for each module's hash recorded
// the active cursor for it.
func (l *Loader) GetAllCursors(ctx context.Context) (out map[string]*sink.Cursor, err error) {
	type cursorRow struct {
		ID       string
		Cursor   string
		BlockNum uint64
		BlockID  string
	}

	rows, err := l.DB.QueryContext(ctx, "SELECT id, cursor, block_num, block_id from cursors")
	if err != nil {
		return nil, fmt.Errorf("query all cursors: %w", err)
	}

	out = make(map[string]*sink.Cursor)
	for rows.Next() {
		c := &cursorRow{}
		if err := rows.Scan(&c.ID, &c.Cursor, &c.BlockNum, &c.BlockID); err != nil {
			return nil, fmt.Errorf("getting all cursors:  %w", err)
		}

		out[c.ID], err = sink.NewCursor(c.Cursor)
		if err != nil {
			return nil, fmt.Errorf("database corrupted: stored cursor %q is not a valid cursor", c.Cursor)
		}
	}

	return out, nil
}

func (l *Loader) GetCursor(ctx context.Context, outputModuleHash string) (*sink.Cursor, error) {
	type cursorRow struct {
		ID       string
		Cursor   string
		BlockNum uint64
		BlockID  string
	}

	query := fmt.Sprintf("SELECT id, cursor, block_num, block_id from cursors WHERE id = '%s'", outputModuleHash)
	row := l.DB.QueryRowContext(ctx, query)

	c := &cursorRow{}
	if err := row.Scan(&c.ID, &c.Cursor, &c.BlockNum, &c.BlockID); err != nil {
		if err == sql.ErrNoRows {
			return sink.NewBlankCursor(), ErrCursorNotFound
		}
		return nil, fmt.Errorf("getting cursor %q:  %w", outputModuleHash, err)
	}

	cursor, err := sink.NewCursor(c.Cursor)
	if err != nil {
		return nil, fmt.Errorf("database corrupted: stored cursor %q is not decodable", c.Cursor)
	}

	return cursor, nil
}

func (l *Loader) InsertCursor(ctx context.Context, moduleHash string, c *sink.Cursor) error {
	query := fmt.Sprintf("INSERT INTO cursors (id, cursor, block_num, block_id) values ('%s', '%s', %d, '%s')", moduleHash, c, c.Block().Num(), c.Block().ID())
	if _, err := l.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("insert cursor: %w", err)
	}

	return nil
}

// UpdateCursor updates the active cursor. If no cursor is active and no update occurred, returns
// ErrCursorNotFound. If the update was not successful on the database, returns an error.
func (l *Loader) UpdateCursor(ctx context.Context, moduleHash string, c *sink.Cursor) error {
	_, err := l.runModifiyCursorQuery(ctx, "update", l.UpdateCursorQuery(moduleHash, c))
	return err
}

func (l *Loader) UpdateCursorQuery(moduleHash string, c *sink.Cursor) string {
	zap.Inline(c)

	return fmt.Sprintf("UPDATE cursors set cursor = '%s', block_num = %d, block_id = '%s' WHERE id = '%s'", c, c.Block().Num(), c.Block().ID(), moduleHash)
}

// DeleteCursor deletes the active cursor for the given 'moduleHash'. If no cursor is active and
// no delete occurrred, returns ErrCursorNotFound. If the delete was not successful on the database, returns an error.
func (l *Loader) DeleteCursor(ctx context.Context, moduleHash string) error {
	_, err := l.runModifiyCursorQuery(ctx, "delete", fmt.Sprintf("DELETE FROM cursors WHERE id = '%s'", moduleHash))
	return err
}

// DeleteAllCursors deletes the active cursor for the given 'moduleHash'. If no cursor is active and
// no delete occurrred, returns ErrCursorNotFound. If the delete was not successful on the database, returns an error.
func (l *Loader) DeleteAllCursors(ctx context.Context) (deletedCount int64, err error) {
	deletedCount, err = l.runModifiyCursorQuery(ctx, "delete", "DELETE FROM cursors")
	if err != nil && errors.Is(err, ErrCursorNotFound) {
		return 0, nil
	}

	return deletedCount, nil
}

func (l *Loader) runModifiyCursorQuery(ctx context.Context, action string, query string) (rowsAffected int64, err error) {
	result, err := l.DB.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("%s cursor: %w", action, err)
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}

	if rowsAffected <= 0 {
		return 0, ErrCursorNotFound
	}

	return rowsAffected, nil
}
