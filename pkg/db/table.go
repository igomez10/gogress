package db

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

type ColumnType int

const (
	ColumnTypeString ColumnType = iota
	ColumnTypeInt
)

type Column struct {
	Name  string
	Type  ColumnType
	Width int // bytes allocated for this column
}

type Schema struct {
	Columns []Column
}

// TotalWidth returns the sum of all column widths.
func (s Schema) TotalWidth() int {
	total := 0
	for _, c := range s.Columns {
		total += c.Width
	}
	return total
}

// PrimaryKey returns the first column name (the primary key).
func (s Schema) PrimaryKey() string {
	if len(s.Columns) == 0 {
		return ""
	}
	return s.Columns[0].Name
}

type Table struct {
	Mutex  sync.RWMutex
	Schema Schema

	KeyOffsets map[string]int64
	Storage    ReadWriteCloserSeeker
}

const paddingSize = 128

type Record struct {
	Columns map[string][]byte
}

// Key returns the primary key value from the record.
func (r Record) Key(schema Schema) []byte {
	if len(schema.Columns) == 0 {
		return nil
	}
	return r.Columns[schema.PrimaryKey()]
}

// Value returns a single-value convenience accessor for two-column schemas (primary key + one value column).
func (r Record) Value(schema Schema) []byte {
	if len(schema.Columns) < 2 {
		return nil
	}
	return r.Columns[schema.Columns[1].Name]
}

func DefaultSchema() Schema {
	return Schema{
		Columns: []Column{
			{Name: "key", Type: ColumnTypeString, Width: 16},
			{Name: "value", Type: ColumnTypeString, Width: 16},
		},
	}
}

func (tb *Table) Put(colValues map[string][]byte, writeToLogHook func()) error {
	// Validate that the primary key column is provided
	pk := tb.Schema.PrimaryKey()
	if pk == "" {
		return fmt.Errorf("schema has no columns")
	}
	if _, ok := colValues[pk]; !ok {
		return fmt.Errorf("missing primary key column %q", pk)
	}

	rec, err := serializeRecord(tb.Schema, colValues)
	if err != nil {
		return err
	}

	tb.Mutex.Lock()
	defer tb.Mutex.Unlock()
	off, err := tb.Storage.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	writeToLogHook()

	sizewritten, err := tb.Storage.Write(rec)
	if err != nil {
		return err
	}
	if sizewritten != len(rec) {
		return io.ErrShortWrite
	}

	if err := tb.Storage.Sync(); err != nil {
		return err
	}

	pkVal := string(bytes.TrimRight(colValues[tb.Schema.PrimaryKey()], "\x00"))
	tb.KeyOffsets[pkVal] = off
	return nil
}

func (tb *Table) Get(key []byte) (Record, bool, error) {
	tb.Mutex.RLock()
	off, ok := tb.KeyOffsets[string(key)]
	tb.Mutex.RUnlock()
	if !ok {
		return Record{}, false, nil
	}

	record, err := ReadRecordFromFile(tb.Storage, off, tb.Schema)
	if err != nil {
		return Record{}, false, err
	}

	return record, true, nil
}

func serializeRecord(schema Schema, colValues map[string][]byte) ([]byte, error) {
	totalWidth := schema.TotalWidth()
	if totalWidth > paddingSize {
		return nil, fmt.Errorf("schema total width %d exceeds record size %d", totalWidth, paddingSize)
	}

	rec := make([]byte, 0, paddingSize)
	for _, col := range schema.Columns {
		val := colValues[col.Name]
		if len(val) > col.Width {
			return nil, fmt.Errorf("column %q value length %d exceeds width %d", col.Name, len(val), col.Width)
		}
		// write value padded to column width
		padded := make([]byte, col.Width)
		copy(padded, val)
		rec = append(rec, padded...)
	}

	// pad to paddingSize
	for len(rec) < paddingSize {
		rec = append(rec, 0)
	}

	return rec, nil
}

func ReadRecordFromFile(f io.ReadSeeker, offset int64, schema Schema) (Record, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return Record{}, err
	}

	buf := make([]byte, paddingSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return Record{}, err
	}

	record := Record{Columns: make(map[string][]byte)}
	pos := 0
	for _, col := range schema.Columns {
		end := pos + col.Width
		if end > len(buf) {
			return Record{}, fmt.Errorf("record too short for schema")
		}
		val := bytes.TrimRight(buf[pos:end], "\x00")
		record.Columns[col.Name] = val
		pos = end
	}

	return record, nil
}

func WriteRecordToFile(f io.WriteSeeker, rec Record, schema Schema) (int64, error) {
	data, err := serializeRecord(schema, rec.Columns)
	if err != nil {
		return 0, err
	}

	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	n, err := f.Write(data)
	if err != nil {
		return 0, err
	}
	if n != len(data) {
		return 0, io.ErrShortWrite
	}

	return off, nil
}
