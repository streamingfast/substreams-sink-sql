package db

import (
	"fmt"
)

// Insert a row in the DB, it is assumed the table exists, you can do a
// check before with HasTable()
func (l *Loader) Insert(tableName string, primaryKey string, data map[string]string) error {
	if _, found := l.entries[tableName]; !found {
		l.entries[tableName] = map[string]*Operation{}
	}
	if _, found := l.entries[tableName][primaryKey]; found {
		return fmt.Errorf("attempting to insert in table %q a private key %q, that is already scheduled for insertion. Insert should only be called once for a given private key", tableName, primaryKey
	}
	// we need to make sure to add the primary key in the data so that
	// it gets created
	data[l.tablePrimaryKeys[tableName]] = primaryKey
	l.entries[tableName][primaryKey] = l.newInsertOperation(tableName, primaryKey, data)
	l.EntriesCount++
	return nil
}

// Update a row in the DB, it is assumed the table exists, you can do a
// check before with HasTable()
func (l *Loader) Update(tableName string, primaryKey string, data map[string]string) error {
	if _, found := l.entries[tableName]; !found {
		l.entries[tableName] = map[string]*Operation{}
	}

	if op, found := l.entries[tableName][primaryKey]; found {
		if op.opType == OperationTypeDelete {
			return fmt.Errorf("attempting to update an object with private key %q, that schedule to be deleted.")
		}

		op.mergeData(data)
		l.entries[tableName][primaryKey] = op
		return nil
	}

	l.entries[tableName][primaryKey] = l.newUpdateOperation(tableName, primaryKey, data)
	l.EntriesCount++
	return nil
}

// Delete a row in the DB, it is assumed the table exists, you can do a
// check before with HasTable()
func (l *Loader) Delete(tableName string, primaryKey string) error {
	if _, found := l.entries[tableName]; !found {
		l.entries[tableName] = map[string]*Operation{}
	}
	if _, found := l.entries[tableName][primaryKey]; !found {
		l.EntriesCount++
	}
	l.entries[tableName][primaryKey] = l.newDeleteOperation(tableName, primaryKey)
	return nil
}
