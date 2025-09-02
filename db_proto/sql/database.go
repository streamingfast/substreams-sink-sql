package sql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	sink "github.com/streamingfast/substreams-sink"
	pbSchema "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"go.uber.org/zap"
)

type Database interface {
	FetchSinkInfo(schemaName string) (*SinkInfo, error)
	UpdateSinkInfoHash(schemaName string, newHash string) error
	StoreSinkInfo(schemaName string, schemaHash string) error

	CreateDatabase(useConstraints bool) error
	WalkMessageDescriptorAndInsert(dm *dynamic.Message, blockNum uint64, blockTimestamp time.Time, parent *Parent) (time.Duration, error)
	InsertBlock(blockNum uint64, hash string, timestamp time.Time) error

	HandleBlocksUndo(lastValidBlockNumber uint64) error

	FetchCursor() (*sink.Cursor, error)
	StoreCursor(cursor *sink.Cursor) error

	BeginTransaction() error
	CommitTransaction() error
	RollbackTransaction()
	Flush() (time.Duration, error)

	DatabaseHash(schemaName string) (uint64, error)

	GetDialect() Dialect

	Clone() Database
	Open() error
}

type BaseDatabase struct {
	logger                *zap.Logger
	mapOutputType         string
	insertStatements      map[string]*sql.Stmt
	RootMessageDescriptor *desc.MessageDescriptor
	useProtoOptions       bool
}

func NewBaseDatabase(moduleOutputType string, rootMessageDescriptor *desc.MessageDescriptor, useProtoOptions bool, logger *zap.Logger) (database *BaseDatabase, err error) {
	logger = logger.Named("database")

	return &BaseDatabase{
		logger:                logger,
		mapOutputType:         moduleOutputType,
		RootMessageDescriptor: rootMessageDescriptor,
		insertStatements:      make(map[string]*sql.Stmt),
		useProtoOptions:       useProtoOptions,
	}, nil
}

func (d *BaseDatabase) BaseClone() *BaseDatabase {
	return &BaseDatabase{
		logger:                d.logger,
		mapOutputType:         d.mapOutputType,
		RootMessageDescriptor: d.RootMessageDescriptor,
		insertStatements:      d.insertStatements,
	}
}

type Parent struct {
	field string
	id    interface{}
}

func (d *BaseDatabase) WalkMessageDescriptorAndInsertWithDialect(dm *dynamic.Message, blockNum uint64, blockTimestamp time.Time, parent *Parent, dialect Dialect, inserter Inserter) (time.Duration, error) {
	if dm == nil {
		return 0, fmt.Errorf("received a nil message")
	}

	var fieldValues []any
	fieldValues = append(fieldValues, blockNum)
	fieldValues = append(fieldValues, blockTimestamp)

	primaryKeyOffset := 2
	if dialect.UseVersionField() {
		fieldValues = append(fieldValues, time.Now().UnixNano())
		primaryKeyOffset += 1
	}

	if dialect.UseDeletedField() {
		fieldValues = append(fieldValues, false)
		primaryKeyOffset += 1
	}

	md := dm.GetMessageDescriptor()
	tableInfo := proto.TableInfo(md)

	if tableInfo == nil && !d.useProtoOptions {
		tableInfo = &pbSchema.Table{
			Name: md.GetName(),
		}
	}

	d.logger.Debug("Walking message descriptor", zap.String("message_descriptor_name", md.GetName()), zap.Any("table_info", tableInfo))
	primaryKey := ""
	if tableInfo != nil {
		if table := dialect.GetTable(tableInfo.Name); table != nil {
			if table.PrimaryKey != nil {
				primaryKey = table.PrimaryKey.Name
				pkValue := dm.GetFieldByName(primaryKey)
				if pkValue == nil {
					return 0, fmt.Errorf("missing primary key field %q for table %q", primaryKey, tableInfo.Name)
				}
				fieldValues = append(fieldValues, pkValue)
			}
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
			// Check if this is an array of messages or native values
			if len(v) > 0 {
				if _, ok := v[0].(*dynamic.Message); ok {
					// Array of messages - process as child tables
					for _, c := range v {
						fm, ok := c.(*dynamic.Message)
						if !ok {
							return 0, fmt.Errorf("Mixed array types not supported in 'from-proto' mode. message %q, field %q", md.GetFullyQualifiedName(), fd.GetName())
						}
						childs = append(childs, fm)
					}
				} else {
					// Array of native values - add as a single field value (the array itself)
					fieldValues = append(fieldValues, fv)
				}
			}
		} else if fm, ok := fv.(*dynamic.Message); ok {
			if fm == nil {
				continue //un-use oneOf field
			}
			childs = append(childs, fm) //need to be handled after current message inserted
		} else {
			fieldValues = append(fieldValues, fv)
		}
	}

	var p *Parent

	if tableInfo != nil {
		insertStartAt := time.Now()
		table := dialect.GetTable(tableInfo.Name)
		if table != nil {
			err := inserter.Insert(table.Name, fieldValues)
			if err != nil {
				return 0, fmt.Errorf("inserting into table %q: %w", table.Name, err)
			}
			if len(childs) > 0 && d.useProtoOptions {
				if table.PrimaryKey == nil {
					return 0, fmt.Errorf("table %q has no primary key and has %d associated children table", table.Name, len(childs))
				}
				id := fieldValues[table.PrimaryKey.Index+primaryKeyOffset]
				p = &Parent{
					field: strings.ToLower(md.GetName()),
					id:    id,
				}
			}
			totalSqlDuration += time.Since(insertStartAt)
		}
	}

	for _, fm := range childs {
		sqlDuration, err := d.WalkMessageDescriptorAndInsertWithDialect(fm, blockNum, blockTimestamp, p, dialect, inserter)
		if err != nil {
			return 0, fmt.Errorf("processing child %q: %w", fm.GetMessageDescriptor().GetFullyQualifiedName(), err)
		}
		totalSqlDuration += sqlDuration
	}

	return totalSqlDuration, nil
}

type SinkInfo struct {
	SchemaHash string `json:"schema_hash"`
}
