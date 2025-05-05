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
	"github.com/streamingfast/substreams-sink-sql/proto"
	"go.uber.org/zap"
)

type Database interface {
	FetchSinkInfo(schemaName string) (*SinkInfo, error)
	UpdateSinkInfoHash(schemaName string, newHash string) error
	StoreSinkInfo(schemaName string, schemaHash string) error

	CreateDatabase(useConstraints bool, schemaName string) error
	SetInserter(inserter Inserter)
	WalkMessageDescriptorAndInsert(dm *dynamic.Message, blockNum uint64, parent *Parent) (time.Duration, error)
	InsertBlock(blockNum uint64, hash string, timestamp time.Time) error

	HandleBlocksUndo(lastValidBlockNumber uint64) error

	FetchCursor() (*sink.Cursor, error)
	StoreCursor(cursor *sink.Cursor) error

	BeginTransaction() error
	CommitTransaction() error
	RollbackTransaction()
	Flush() (time.Duration, error)

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
	Dialect               Dialect
	Inserter              Inserter
}

func NewBaseDatabase(sqlDialect Dialect, db *sql.DB, moduleOutputType string, rootMessageDescriptor *desc.MessageDescriptor, logger *zap.Logger) (database *BaseDatabase, err error) {
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

func (d *BaseDatabase) Flush() (time.Duration, error) {
	startFlush := time.Now()
	err := d.Inserter.Flush(d.Tx)
	if err != nil {
		return 0, fmt.Errorf("flushing: %w", err)
	}
	return time.Since(startFlush), nil
}

func (d *BaseDatabase) WrapInsertStatement(stmt *sql.Stmt) *sql.Stmt {
	if d.Tx != nil {
		stmt = d.Tx.Stmt(stmt)
	}
	return stmt
}

type Parent struct {
	field string
	id    interface{}
}

func (d *BaseDatabase) WalkMessageDescriptorAndInsert(dm *dynamic.Message, blockNum uint64, parent *Parent) (time.Duration, error) {
	if dm == nil {
		return 0, fmt.Errorf("received a nil message")
	}

	var fieldValues []any
	fieldValues = append(fieldValues, blockNum)

	md := dm.GetMessageDescriptor()
	tableInfo := proto.TableInfo(md)

	primaryKey := ""
	if tableInfo != nil {
		table := d.Dialect.GetTable(tableInfo.Name)
		if table.PrimaryKey != nil {
			primaryKey = table.PrimaryKey.Name
			pkValue := dm.GetFieldByName(primaryKey)
			if pkValue == nil {
				return 0, fmt.Errorf("missing primary key field %q for table %q", primaryKey, tableInfo.Name)
			}
			fieldValues = append(fieldValues, pkValue)
		}
	}

	totalSqlDuration := time.Duration(0)

	if parent != nil {
		fieldValues = append(fieldValues, parent.id)
	}

	var childs []*dynamic.Message

	for _, fd := range dm.GetKnownFields() {
		if fd.GetName() == primaryKey {
			continue
		}
		fv := dm.GetField(fd)
		if v, ok := fv.([]interface{}); ok {
			for _, c := range v {
				fm, ok := c.(*dynamic.Message)
				if !ok {
					panic("expected *dynamic.Message")
				}
				childs = append(childs, fm) //n
			}
		} else if fm, ok := fv.(*dynamic.Message); ok {
			if fm == nil {
				continue //un-use oneOf field
			}
			childs = append(childs, fm) //need to be handled after current message inserted
			//sqlDuration, err := d.WalkMessageDescriptorAndInsert(fm, blockNum, nil)
			//if err != nil {
			//	return 0, fmt.Errorf("walking nested message descriptor %q: %w", fd.GetName(), err)
			//}
			//totalSqlDuration += sqlDuration
		} else {
			fieldValues = append(fieldValues, fv)
		}
	}

	var p *Parent

	if tableInfo != nil {
		insertStartAt := time.Now()
		table := d.Dialect.GetTable(tableInfo.Name)
		err := d.Inserter.Insert(table.Name, fieldValues, d.WrapInsertStatement)
		if err != nil {
			fmt.Printf("fieldValues: %v\n", fieldValues)
			return 0, fmt.Errorf("inserting into table %q: %w", table.Name, err)
		}
		if len(childs) > 0 {
			if table.PrimaryKey == nil {
				return 0, fmt.Errorf("table %q has no primary key and has %d associated children table", table.Name, len(childs))
			}
			id := fieldValues[table.PrimaryKey.Index+1]
			p = &Parent{
				field: strings.ToLower(md.GetName()),
				id:    id,
			}
		}
		totalSqlDuration += time.Since(insertStartAt)
	}

	for _, fm := range childs {
		sqlDuration, err := d.WalkMessageDescriptorAndInsert(fm, blockNum, p)
		if err != nil {
			return 0, fmt.Errorf("processing child %q: %w", fm.GetMessageDescriptor().GetFullyQualifiedName(), err)
		}
		totalSqlDuration += sqlDuration
	}

	return totalSqlDuration, nil
}

func (d *BaseDatabase) SetInserter(inserter Inserter) {
	d.Inserter = inserter
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
