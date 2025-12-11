package db

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type TypeGetter func(tableName string, columnName string) (reflect.Type, error)

type Queryable interface {
	query(d Dialect) (string, error)
}

type OperationType string

const (
	OperationTypeInsert      OperationType = "INSERT"
	OperationTypeUpsert      OperationType = "UPSERT"
	OperationTypeUpdate      OperationType = "UPDATE"
	OperationTypeDelete      OperationType = "DELETE"
	OperationTypeDeltaUpsert OperationType = "DELTA_UPSERT" // For numeric delta updates: balance = balance + delta
)

type Operation struct {
	table              *TableInfo
	opType             OperationType
	primaryKey         map[string]string
	data               map[string]string
	ordinal            uint64
	reversibleBlockNum *uint64 // nil if that block is known to be irreversible
}

func (o *Operation) String() string {
	return fmt.Sprintf("%s/%s (%s)", o.table.identifier, createRowUniqueID(o.primaryKey), strings.ToLower(string(o.opType)))
}

func (l *Loader) newInsertOperation(table *TableInfo, primaryKey map[string]string, data map[string]string, ordinal uint64, reversibleBlockNum *uint64) *Operation {
	return &Operation{
		table:              table,
		opType:             OperationTypeInsert,
		primaryKey:         primaryKey,
		data:               data,
		ordinal:            ordinal,
		reversibleBlockNum: reversibleBlockNum,
	}
}

func (l *Loader) newUpsertOperation(table *TableInfo, primaryKey map[string]string, data map[string]string, ordinal uint64, reversibleBlockNum *uint64) *Operation {
	return &Operation{
		table:              table,
		opType:             OperationTypeUpsert,
		primaryKey:         primaryKey,
		data:               data,
		ordinal:            ordinal,
		reversibleBlockNum: reversibleBlockNum,
	}
}

func (l *Loader) newUpdateOperation(table *TableInfo, primaryKey map[string]string, data map[string]string, ordinal uint64, reversibleBlockNum *uint64) *Operation {
	return &Operation{
		table:              table,
		opType:             OperationTypeUpdate,
		primaryKey:         primaryKey,
		data:               data,
		ordinal:            ordinal,
		reversibleBlockNum: reversibleBlockNum,
	}
}

func (l *Loader) newDeleteOperation(table *TableInfo, primaryKey map[string]string, ordinal uint64, reversibleBlockNum *uint64) *Operation {
	return &Operation{
		table:              table,
		opType:             OperationTypeDelete,
		primaryKey:         primaryKey,
		ordinal:            ordinal,
		reversibleBlockNum: reversibleBlockNum,
	}
}

func (l *Loader) newDeltaUpsertOperation(table *TableInfo, primaryKey map[string]string, data map[string]string, ordinal uint64, reversibleBlockNum *uint64) *Operation {
	return &Operation{
		table:              table,
		opType:             OperationTypeDeltaUpsert,
		primaryKey:         primaryKey,
		data:               data,
		ordinal:            ordinal,
		reversibleBlockNum: reversibleBlockNum,
	}
}

func (o *Operation) mergeData(newData map[string]string) error {
	if o.opType == OperationTypeDelete {
		return fmt.Errorf("unable to merge data for a delete operation")
	}

	for k, v := range newData {
		o.data[k] = v
	}
	return nil
}

// mergeOperation merges another operation into this one, keeping the lowest ordinal
func (o *Operation) mergeOperation(otherData map[string]string) error {
	if o.opType == OperationTypeDelete {
		return fmt.Errorf("unable to merge operation for a delete operation")
	}

	return o.mergeData(otherData)
}

// accumulateDeltas adds numeric delta values together for the same primary key
// Used when multiple delta operations target the same row within a batch
// Values are signed (negative for subtract, positive for add)
func (o *Operation) accumulateDeltas(newData map[string]string) {
	for k, v := range newData {
		// Skip primary key columns - they don't change
		isPK := false
		for pk := range o.primaryKey {
			if k == pk {
				isPK = true
				break
			}
		}
		if isPK {
			continue
		}

		existingStr, exists := o.data[k]
		if !exists {
			o.data[k] = v
			continue
		}

		// Parse as signed decimals and add them (sign is embedded in value)
		existingDec, err1 := parseDecimal(existingStr)
		newDec, err2 := parseDecimal(v)
		if err1 == nil && err2 == nil {
			o.data[k] = existingDec.Add(newDec).String()
		} else {
			// Not numeric, just replace (shouldn't happen for deltas)
			o.data[k] = v
		}
	}
}

func parseDecimal(s string) (decimal, error) {
	// Simple decimal parsing - just use big.Rat for precision
	var d decimal
	_, ok := d.SetString(s)
	if !ok {
		return decimal{}, fmt.Errorf("invalid decimal: %s", s)
	}
	return d, nil
}

// decimal is a simple wrapper around big.Rat for delta accumulation
type decimal struct {
	*big.Rat
}

func (d *decimal) SetString(s string) (*decimal, bool) {
	if d.Rat == nil {
		d.Rat = new(big.Rat)
	}
	_, ok := d.Rat.SetString(s)
	return d, ok
}

func (d decimal) Add(other decimal) decimal {
	result := new(big.Rat)
	result.Add(d.Rat, other.Rat)
	return decimal{result}
}

func (d decimal) String() string {
	return d.Rat.FloatString(18)
}

var integerRegex = regexp.MustCompile(`^\d+$`)
var dateRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
var reflectTypeTime = reflect.TypeOf(time.Time{})

func EscapeIdentifier(valueToEscape string) string {
	if strings.Contains(valueToEscape, `"`) {
		valueToEscape = strings.ReplaceAll(valueToEscape, `"`, `""`)
	}

	return `"` + valueToEscape + `"`
}

func escapeStringValue(valueToEscape string) string {
	if strings.Contains(valueToEscape, `'`) {
		valueToEscape = strings.ReplaceAll(valueToEscape, `'`, `''`)
	}

	return `'` + valueToEscape + `'`
}

// to store in an history table
func primaryKeyToJSON(primaryKey map[string]string) string {
	m, err := json.Marshal(primaryKey)
	if err != nil {
		panic(err) // should never happen with map[string]string
	}
	return string(m)
}

// to store in an history table
func jsonToPrimaryKey(in string) (map[string]string, error) {
	out := make(map[string]string)
	err := json.Unmarshal([]byte(in), &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
