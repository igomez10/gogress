package db

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// import
type ReadWriteCloserSeeker interface {
	io.Reader
	io.Writer
	io.Closer
	io.Seeker
	Sync() error
	Name() string
}

type MockReadWriteCloserSeeker struct {
	data   []byte
	offset int64
}

func (m *MockReadWriteCloserSeeker) Read(p []byte) (n int, err error) {
	if m.offset >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.offset:])
	m.offset += int64(n)
	return n, nil
}

func (m *MockReadWriteCloserSeeker) Write(p []byte) (n int, err error) {
	m.data = append(m.data[:m.offset], p...)
	m.offset += int64(len(p))
	return len(p), nil
}

func (m *MockReadWriteCloserSeeker) Close() error {
	return nil
}

func (m *MockReadWriteCloserSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		m.offset = offset
	case io.SeekCurrent:
		m.offset += offset
	case io.SeekEnd:
		m.offset = int64(len(m.data)) + offset
	}
	if m.offset < 0 {
		return 0, io.EOF
	}
	return m.offset, nil
}

func (m *MockReadWriteCloserSeeker) Sync() error {
	return nil
}

func (m *MockReadWriteCloserSeeker) Name() string {
	return "mock"
}

type NewDBOptions struct {
	File    ReadWriteCloserSeeker
	LogFile io.ReadWriter
}

func NewDB(opts NewDBOptions) (*DB, error) {
	db := &DB{
		LogFile: opts.LogFile,
		Tables:  make(map[string]*Table),
	}
	// create default table
	db.Tables["default"] = &Table{
		KeyOffsets: make(map[string]int64),
		Storage:    opts.File,
	}

	if db.Tables["default"].Storage == nil {
		// create but not truncate
		f, err := os.OpenFile("/tmp/gogressdb_default", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		db.Tables["default"].Storage = f
	}

	if db.LogFile == nil {
		db.LogFile = os.Stdout
	}

	// load index from file
	m, err := LoadOffsetsFromFile(db.LogFile)
	for tableName, idx := range m {
		db.Tables[tableName].KeyOffsets = idx
	}
	if err != nil {
		return nil, err
	}

	return db, nil
}

var InvalidLogLineError = errors.New("invalid log line")

// format is tableName key value
func LoadOffsetsFromFile(reader io.Reader) (map[string]map[string]int64, error) {
	idx := map[string]map[string]int64{}
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			return nil, InvalidLogLineError
		}
		tableName := parts[0]
		key := parts[1]
		off, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, InvalidLogLineError
		}
		if idx[tableName] == nil {
			idx[tableName] = map[string]int64{}
		}
		idx[tableName][key] = off
	}
	return idx, nil
}

type DB struct {
	LogFile io.ReadWriter

	// KeyOffsets is a mapping from keys to their file offsets
	// this allows for quick lookups when retrieving values
	// and efficient deletion of records
	// the key is the record's key, basically the primary key
	// the value is the record's value, which is the file offset
	// the value is the record's value, which is the file offset
	// of the last record for that key. by offset we mean the byte offset
	// from the beginning of the file to the start of the record.
	Mutex sync.RWMutex

	Tables map[string]*Table
}

func (db *DB) ListTables() []string {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()

	var tables []string
	for name := range db.Tables {
		tables = append(tables, name)
	}
	return tables
}

func (db *DB) Put(tableName, key, val []byte) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	table, ok := db.Tables[string(tableName)]
	if !ok {
		return fmt.Errorf("table %q not found", tableName)
	}

	hook := func(off int64) {
		// write to log for crash recovery
		fmt.Fprintln(db.LogFile, string(tableName)+":"+string(key)+":"+fmt.Sprintf("%d", off))
	}

	return table.Put(key, val, hook)
}

func (db *DB) Get(tableName, key []byte) ([]byte, bool, error) {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()

	table, ok := db.Tables[string(tableName)]
	if !ok {
		return nil, false, fmt.Errorf("table %q not found", tableName)
	}

	return table.Get(key)
}

type Table struct {
	KeyOffsets map[string]int64
	Mutex      sync.RWMutex
	Storage    ReadWriteCloserSeeker
}

const paddingSize = 16

func (tb *Table) Put(key, val []byte, writeToLogHook func(int64)) error {
	// Create a new record
	if len(key)+len(val)+1 > paddingSize {
		return fmt.Errorf("record too large")
	}
	rec := make([]byte, 0, paddingSize)
	rec = append(rec, key...)
	rec = append(rec, []byte(":")...)
	rec = append(rec, val...)

	// fill remaining space with null bytes
	for i := len(key) + len(val) + 1; i < paddingSize; i++ {
		rec = append(rec, 0)
	}

	tb.Mutex.Lock()
	defer tb.Mutex.Unlock()
	off, err := tb.Storage.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// TODO move to db
	writeToLogHook(off)

	// write record to file
	sizewritten, err := tb.Storage.Write(rec)
	if err != nil {
		return err
	}
	if sizewritten != len(rec) {
		return io.ErrShortWrite
	}

	// flush on every write
	if err := tb.Storage.Sync(); err != nil {
		return err
	}

	// update offset for key
	tb.KeyOffsets[string(key)] = off
	return nil
}

func (tb *Table) Get(key []byte) ([]byte, bool, error) {
	tb.Mutex.RLock()
	off, ok := tb.KeyOffsets[string(key)]
	tb.Mutex.RUnlock()
	if !ok {
		return nil, false, nil
	}

	if _, err := tb.Storage.Seek(off, io.SeekStart); err != nil {
		return nil, false, err
	}

	reader := io.LimitReader(tb.Storage, paddingSize)
	buf := make([]byte, paddingSize)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, false, err
	}
	// trim null bytes
	buf = bytes.TrimRight(buf, "\x00")
	// split by first colon
	parts := bytes.SplitN(buf, []byte(":"), 2)
	if len(parts) != 2 {
		return nil, false, fmt.Errorf("invalid record format")
	}
	return parts[1], true, nil
}
