package db

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jimsmart/schema"

	"go.uber.org/zap"
)

type Loader struct {
	*sql.DB

	schema string

	tableRegistry    map[[2]string]map[string]reflect.Type
	tablePrimaryKeys map[[2]string]string
	logger           *zap.Logger
}

func NewLoader(psqlDsn string, logger *zap.Logger) (*Loader, error) {
	dsn, err := parseDSN(psqlDsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	db, err := sql.Open("postgres", dsn.connString())
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w")
	}

	return &Loader{
		DB:               db,
		schema:           dsn.schema,
		tableRegistry:    map[[2]string]map[string]reflect.Type{},
		tablePrimaryKeys: map[[2]string]string{},
		logger:           logger,
	}, nil
}

func (l *Loader) LoadTables() error {
	schemaTables, err := schema.Tables(l.DB)
	if err != nil {
		return fmt.Errorf("retrieving table and schema: %w", err)
	}
	for keys, tables := range schemaTables {
		l.logger.Info("key", zap.Reflect("k", keys), zap.Int("count", len(tables)))
		if keys[0] != l.schema {
			continue
		}

		m := map[string]reflect.Type{}
		for _, f := range tables {
			m[f.Name()] = f.ScanType()
		}
		l.tableRegistry[keys] = m

		key, err := schema.PrimaryKey(l.DB, keys[0], keys[1])
		if err != nil {
			return fmt.Errorf("get primary key: %w", err)
		}
		if len(key) > 0 {
			l.tablePrimaryKeys[keys] = key[0]
		} else {
			l.tablePrimaryKeys[keys] = "id"
		}
	}
	return nil
}
