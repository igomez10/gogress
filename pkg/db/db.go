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
		Storage:    opts.File,
		LogFile:    opts.LogFile,
		KeyOffsets: make(map[string]int64),
	}

	if db.Storage == nil {
		// create but not truncate
		f, err := os.OpenFile("/tmp/gogressdb", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		db.Storage = f
	}

	if db.LogFile == nil {
		db.LogFile = os.Stdout
	}

	// load index from file
	m, err := LoadOffsetsFromFile(db.LogFile)
	if err != nil {
		return nil, err
	}

	db.KeyOffsets = m

	return db, nil
}

var InvalidLogLineError = errors.New("invalid log line")

func LoadOffsetsFromFile(reader io.Reader) (map[string]int64, error) {
	idx := map[string]int64{}
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return nil, InvalidLogLineError
		}
		key := parts[0]
		off, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, InvalidLogLineError
		}
		idx[key] = off
	}
	return idx, nil
}

type DB struct {
	Storage ReadWriteCloserSeeker
	LogFile io.ReadWriter

	// KeyOffsets is a mapping from keys to their file offsets
	// this allows for quick lookups when retrieving values
	// and efficient deletion of records
	// the key is the record's key, basically the primary key
	// the value is the record's value, which is the file offset
	// the value is the record's value, which is the file offset
	// of the last record for that key. by offset we mean the byte offset
	// from the beginning of the file to the start of the record.
	KeyOffsets map[string]int64 // key -> file offset of last record
	Mutex      sync.RWMutex
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

	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	off, err := db.Storage.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// write to log for crash recovery
	if db.LogFile != nil {
		fmt.Fprintln(db.LogFile, string(key)+":"+fmt.Sprintf("%d", off))
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
	db.KeyOffsets[string(key)] = off
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.Mutex.RLock()
	off, ok := db.KeyOffsets[string(key)]
	db.Mutex.RUnlock()
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
