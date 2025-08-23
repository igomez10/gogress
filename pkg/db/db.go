package db

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
		Storage: opts.File,
		LogFile: opts.LogFile,
		Idx:     make(map[string]int64),
	}

	if db.Storage == nil {
		f, err := os.Create("/tmp/gogressdb")
		if err != nil {
			return nil, err
		}
		db.Storage = f
	}

	if db.LogFile == nil {
		db.LogFile = os.Stdout
	}

	return db, nil
}

type DB struct {
	Storage ReadWriteCloserSeeker
	LogFile io.Writer

	// Idx is a mapping from keys to their file offsets
	// this allows for quick lookups when retrieving values
	// and efficient deletion of records
	// the key is the record's key, basically the primary key
	// the value is the record's value, which is the file offset
	// the value is the record's value, which is the file offset
	// of the last record for that key. by offset we mean the byte offset
	// from the beginning of the file to the start of the record.
	Idx map[string]int64 // key -> file offset of last record
	Mu  sync.RWMutex
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

	db.Mu.Lock()
	defer db.Mu.Unlock()
	off, err := db.Storage.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// write to log for crash recovery
	if db.LogFile != nil {
		fmt.Fprintln(db.LogFile, string(rec))
	}

	// write record to file
	sizewritten, err := db.Storage.Write(rec)
	if err != nil {
		return err
	}
	if sizewritten != len(rec) {
		return io.ErrShortWrite
	}

	// flush on every write
	if err := db.Storage.Sync(); err != nil {
		return err
	}

	// update offset for key
	db.Idx[string(key)] = off
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.Mu.RLock()
	off, ok := db.Idx[string(key)]
	db.Mu.RUnlock()
	if !ok {
		return nil, false, nil
	}

	if _, err := db.Storage.Seek(off, io.SeekStart); err != nil {
		return nil, false, err
	}

	reader := io.LimitReader(db.Storage, paddingSize)
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
