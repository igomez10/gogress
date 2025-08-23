package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
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
	LogFile io.Writer
}

func NewDB(opts NewDBOptions) (*DB, error) {
	db := &DB{
		storage: opts.File,
		logFile: opts.LogFile,
		idx:     make(map[string]int64),
	}

	if db.storage == nil {
		f, err := os.Create("/tmp/gogressdb")
		if err != nil {
			return nil, err
		}
		db.storage = f
	}

	if db.logFile == nil {
		db.logFile = os.Stdout
	}

	return db, nil
}

type DB struct {
	storage ReadWriteCloserSeeker
	logFile io.Writer

	// idx is a mapping from keys to their file offsets
	// this allows for quick lookups when retrieving values
	// and efficient deletion of records
	// the key is the record's key, basically the primary key
	// the value is the record's value, which is the file offset
	// the value is the record's value, which is the file offset
	// of the last record for that key. by offset we mean the byte offset
	// from the beginning of the file to the start of the record.
	idx map[string]int64 // key -> file offset of last record
	mu  sync.RWMutex
}

const paddingSize = 64

func (db *DB) Put(key, val []byte) error {
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

	db.mu.Lock()
	defer db.mu.Unlock()
	off, err := db.storage.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// write to log for crash recovery
	if db.logFile != nil {
		fmt.Fprintln(db.logFile, string(rec))
	}

	// write record to file
	sizewritten, err := db.storage.Write(rec)
	if err != nil {
		return err
	}
	if sizewritten != len(rec) {
		return io.ErrShortWrite
	}

	// flush on every write
	if err := db.storage.Sync(); err != nil {
		return err
	}

	// update offset for key
	db.idx[string(key)] = off
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	off, ok := db.idx[string(key)]
	db.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}

	if _, err := db.storage.Seek(off, io.SeekStart); err != nil {
		return nil, false, err
	}

	reader := io.LimitReader(db.storage, paddingSize)
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
