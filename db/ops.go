package db

import (
	"fmt"

	"go.uber.org/zap"
)

// Insert a row in the DB, it is assumed the table exists, you can do a
// check before with HasTable()
func (l *Loader) Insert(tableName string, primaryKey string, data map[string]string) error {
	if l.tracer.Enabled() {
		l.logger.Debug("processing insert operation", zap.String("table_name", tableName), zap.String("primary_key", primaryKey), zap.Int("field_count", len(data)))
	}

	table, found := l.tables[tableName]
	if !found {
		return fmt.Errorf("unknown table %q", tableName)
	}

	if _, found := l.entries[tableName]; !found {
		if l.tracer.Enabled() {
			l.logger.Debug("adding tracking of table never seen before", zap.String("table_name", tableName))
		}

		l.entries[tableName] = map[string]*Operation{}
	}

	if _, found := l.entries[tableName][primaryKey]; found {
		return fmt.Errorf("attempting to insert in table %q a primary key %q, that is already scheduled for insertion, insert should only be called once for a given primary key", tableName, primaryKey)
	}

	if l.tracer.Enabled() {
		l.logger.Debug("primary key entry never existed for table, adding insert operation", zap.String("primary_key", primaryKey), zap.String("table_name", tableName))
	}

	// we need to make sure to add the primary key in the data so that it gets created
	data[table.primaryColumn.name] = primaryKey
	l.entries[tableName][primaryKey] = l.newInsertOperation(table, primaryKey, data)
	l.entriesCount++
	return nil
}

// Update a row in the DB, it is assumed the table exists, you can do a
// check before with HasTable()
func (l *Loader) Update(tableName string, primaryKey string, data map[string]string) error {
	if l.tracer.Enabled() {
		l.logger.Debug("processing update operation", zap.String("table_name", tableName), zap.String("primary_key", primaryKey), zap.Int("field_count", len(data)))
	}

	table, found := l.tables[tableName]
	if !found {
		return fmt.Errorf("unknown table %q", tableName)
	}

	if _, found := l.entries[tableName]; !found {
		if l.tracer.Enabled() {
			l.logger.Debug("adding tracking of table never seen before", zap.String("table_name", tableName))
		}

		l.entries[tableName] = map[string]*Operation{}
	}

	if op, found := l.entries[tableName][primaryKey]; found {
		if op.opType == OperationTypeDelete {
			return fmt.Errorf("attempting to update an object with primary key %q, that schedule to be deleted", primaryKey)
		}

		if l.tracer.Enabled() {
			l.logger.Debug("primary key entry already exist for table, merging fields together", zap.String("primary_key", primaryKey), zap.String("table_name", tableName))
		}

		op.mergeData(data)
		l.entries[tableName][primaryKey] = op
		return nil
	} else {
		l.entriesCount++
	}

	if l.tracer.Enabled() {
		l.logger.Debug("primary key entry never existed for table, adding update operation", zap.String("primary_key", primaryKey), zap.String("table_name", tableName))
	}

	l.entries[tableName][primaryKey] = l.newUpdateOperation(table, primaryKey, data)
	return nil
}

// Delete a row in the DB, it is assumed the table exists, you can do a
// check before with HasTable()
func (l *Loader) Delete(tableName string, primaryKey string) error {
	if l.tracer.Enabled() {
		l.logger.Debug("processing delete operation", zap.String("table_name", tableName), zap.String("primary_key", primaryKey))
	}

	table, found := l.tables[tableName]
	if !found {
		return fmt.Errorf("unknown table %q", tableName)
	}

	if _, found := l.entries[tableName]; !found {
		if l.tracer.Enabled() {
			l.logger.Debug("adding tracking of table never seen before", zap.String("table_name", tableName))
		}

		l.entries[tableName] = map[string]*Operation{}
	}

	if _, found := l.entries[tableName][primaryKey]; !found {
		if l.tracer.Enabled() {
			l.logger.Debug("primary key entry never existed for table", zap.String("primary_key", primaryKey), zap.String("table_name", tableName))
		}

		l.entriesCount++
	}

	if l.tracer.Enabled() {
		l.logger.Debug("adding deleting operation", zap.String("primary_key", primaryKey), zap.String("table_name", tableName))
	}

	l.entries[tableName][primaryKey] = l.newDeleteOperation(table, primaryKey)
	return nil
}
