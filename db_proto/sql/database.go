package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/dialect"
	"github.com/streamingfast/substreams-sink-sql/db_proto/stats"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"go.uber.org/zap"
)

type Database interface {
	FetchSinkInfo(schemaName string) (*SinkInfo, error)
	UpdateSinkInfoHash(schemaName string, newHash string) error
	StoreSinkInfo(schemaName string, schemaHash string) error

	CreateDatabase(useConstraints bool, schemaName string) error
	PrepareStatements() error
	WalkMessageDescriptorAndInsert(dm *dynamic.Message, blockId int, parent *Parent, stats *stats.Stats) (interface{}, time.Duration, error)
	InsertBlock(blockNum uint64, hash string, timestamp time.Time) (blockDbId int, err error)

	HandleBlocksUndo(lastValidBlockNum uint64, cursor *sink.Cursor) error

	FetchCursor() (*sink.Cursor, error)
	InsertCursor(cursor *sink.Cursor) error

	BeginTransaction() error
	CommitTransaction() error
	RollbackTransaction()

	DatabaseHash(schemaName string) (uint64, error)

	Clone() Database
}

type BaseDatabase struct {
	DB                    *sql.DB
	logger                *zap.Logger
	mapOutputType         string
	insertStatements      map[string]*sql.Stmt
	RootMessageDescriptor *desc.MessageDescriptor
	Tx                    *sql.Tx
	Dialect               dialect.Dialect
}

func NewBaseDatabase(sqlDialect dialect.Dialect, db *sql.DB, moduleOutputType string, rootMessageDescriptor *desc.MessageDescriptor, logger *zap.Logger) (database *BaseDatabase, err error) {
	logger = logger.Named("database")

	if reachable, err := isDatabaseReachable(db); !reachable {
		return nil, fmt.Errorf("database not reachable: %w", err)
	}

	return &BaseDatabase{
		Dialect:               sqlDialect,
		DB:                    db,
		logger:                logger,
		mapOutputType:         moduleOutputType,
		RootMessageDescriptor: rootMessageDescriptor,
		insertStatements:      make(map[string]*sql.Stmt),
	}, nil
}

func (d *BaseDatabase) CreateDatabase(useConstraints bool, schemaName string) error {

	err := d.Dialect.CreateDatabase(d.Tx)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	if useConstraints {
		err = d.Dialect.ApplyConstraints(d.Tx)
		if err != nil {
			return fmt.Errorf("applying constraints: %w", err)
		}
	}

	return nil
}

func (d *BaseDatabase) BaseClone() *BaseDatabase {
	return &BaseDatabase{
		Dialect:               d.Dialect,
		DB:                    d.DB,
		logger:                d.logger,
		mapOutputType:         d.mapOutputType,
		RootMessageDescriptor: d.RootMessageDescriptor,
		insertStatements:      d.insertStatements,
	}
}

func (d *BaseDatabase) BeginTransaction() (err error) {
	d.Tx, err = d.DB.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	return nil
}

func (d *BaseDatabase) CommitTransaction() (err error) {
	err = d.Tx.Commit()
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	d.Tx = nil
	return nil
}

func (d *BaseDatabase) RollbackTransaction() {
	err := d.Tx.Rollback()
	if err != nil {
		panic("RollbackTransaction failed: " + err.Error())
	}
}

func (d *BaseDatabase) WrapInsertStatement(table string) *sql.Stmt {
	stmt, found := d.insertStatements[table]
	if !found {
		panic(fmt.Sprintf("insert statement not found for table %q", table))
	}
	if d.Tx != nil {
		stmt = d.Tx.Stmt(stmt)
	}
	return stmt
}

func (d *BaseDatabase) WalkMessageDescriptorAndInsert(dm *dynamic.Message, blockId int, parent *Parent, stats *stats.Stats) (interface{}, time.Duration, error) {
	if dm == nil {
		return 0, 0, fmt.Errorf("received a nil message")
	}

	totalSqlDuration := time.Duration(0)
	var fieldValues []any
	fieldValues = append(fieldValues, blockId)

	if parent != nil {
		fieldValues = append(fieldValues, parent.id)
	}

	var childs [][]interface{}
	for _, fd := range dm.GetKnownFields() {
		fv := dm.GetField(fd)
		if v, ok := fv.([]interface{}); ok {
			childs = append(childs, v) //need to be handled after current message inserted
		} else if fm, ok := fv.(*dynamic.Message); ok {
			if fm == nil {
				fieldValues = append(fieldValues, nil)
				continue //un-use oneOf field
			}
			id, sqlDuration, err := d.WalkMessageDescriptorAndInsert(fm, blockId, nil, stats)
			if err != nil {
				return 0, 0, fmt.Errorf("walking nested message descriptor %q: %w", fd.GetName(), err)
			}
			totalSqlDuration += sqlDuration
			fieldValues = append(fieldValues, id)
		} else {
			fieldValues = append(fieldValues, fv)
		}
	}

	md := dm.GetMessageDescriptor()
	var p *Parent
	tableInfo := proto.TableInfo(md)
	var id interface{}
	if tableInfo != nil {
		insertStartAt := time.Now()
		table := d.Dialect.GetTable(tableInfo.Name)
		tableFullName := d.Dialect.FullTableName(table)
		stmt := d.WrapInsertStatement(table.Name)

		row := stmt.QueryRow(fieldValues...)
		err := row.Err()
		if err != nil {
			insert := d.Dialect.GetInsert(tableFullName)
			return 0, 0, fmt.Errorf("querying insert %q: %w", insert, err)
		}
		err = row.Scan(&id)

		p = &Parent{
			field: strings.ToLower(md.GetName()),
			id:    id,
		}
		totalSqlDuration += time.Since(insertStartAt)

	}

	for _, child := range childs {
		for _, c := range child {
			fm, ok := c.(*dynamic.Message)
			if !ok {
				panic("expected *dynamic.Message")
			}
			_, sqlDuration, err := d.WalkMessageDescriptorAndInsert(fm, blockId, p, stats)
			if err != nil {
				return 0, 0, fmt.Errorf("processing child %q: %w", fm.GetMessageDescriptor().GetFullyQualifiedName(), err)
			}
			totalSqlDuration += sqlDuration
		}
	}

	return id, totalSqlDuration, nil
}

type Parent struct {
	field string
	id    interface{}
}

func (d *BaseDatabase) PrepareStatements() error {
	for n, s := range d.Dialect.GetInserts() {
		stmt, err := d.DB.Prepare(s)
		if err != nil {
			return fmt.Errorf("preparing statement %q: %w", s, err)
		}
		d.insertStatements[n] = stmt
	}

	return nil
}

type SinkInfo struct {
	SchemaHash string `json:"schema_hash"`
}

func isDatabaseReachable(db *sql.DB) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := db.PingContext(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}
