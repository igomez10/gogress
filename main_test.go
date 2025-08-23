package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
)

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

type DB struct {
	f ReadWriteCloserSeeker
	// *os.File

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
	off, err := db.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	sizewritten, err := db.f.Write(rec)
	if err != nil {
		return err
	}
	if sizewritten != len(rec) {
		return io.ErrShortWrite
	}
	if err := db.f.Sync(); err != nil {
		return err
	}
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

	if _, err := db.f.Seek(off, io.SeekStart); err != nil {
		return nil, false, err
	}

	reader := io.LimitReader(db.f, paddingSize)
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

func TestBasicPutGet(t *testing.T) {
	// try to remove /tmp/db-
	os.Remove("/tmp/gogressdb")

	db := &DB{
		idx: make(map[string]int64),
	}
	f, err := os.Create("/tmp/gogressdb")
	if err != nil {
		t.Fatal(err)
	}
	db.f = f
	defer os.Remove(db.f.Name())

	key := []byte("key1")
	val := []byte("value1")
	if err := db.Put(key, val); err != nil {
		t.Fatal(err)
	}

	got, ok, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected value to be found")
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("expected %q, got %q", val, got)
	}
}

func TestPut(t *testing.T) {
	db := &DB{
		idx: make(map[string]int64),
	}
	f, err := os.Create("/tmp/gogressdb")
	if err != nil {
		t.Fatal(err)
	}
	db.f = f
	defer os.Remove(db.f.Name())

	key := []byte("key1")
	val := []byte("value1")
	if err := db.Put(key, val); err != nil {
		t.Fatal(err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	// check that value was written
	content, err := io.ReadAll(db.f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, val) {
		t.Fatalf("expected %q to be in %q", val, content)
	}
}

func TestWriteToFile(t *testing.T) {
	f, err := os.Create("/tmp/gogressdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	if _, err := f.Write([]byte("aloha")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	// Verify that the record was written correctly
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, []byte("aloha")) {
		t.Fatalf("expected %q to be in %q", []byte("aloha"), content)
	}
}

func TestOverWriteExistingKey(t *testing.T) {
	db := &DB{
		idx: make(map[string]int64),
	}
	f, err := os.Create("/tmp/gogressdb")
	if err != nil {
		t.Fatal(err)
	}
	db.f = f
	defer os.Remove(db.f.Name())

	key := []byte("key1")
	val1 := []byte("value1")
	if err := db.Put(key, val1); err != nil {
		t.Fatal(err)
	}

	val2 := []byte("value2")
	if err := db.Put(key, val2); err != nil {
		t.Fatal(err)
	}

	got, ok, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected value to be found")
	}
	if !bytes.Equal(got, val2) {
		t.Fatalf("expected %q, got %q", val2, got)
	}
	// expect to contain first value too in underlying file
	if _, err := db.f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(db.f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, val1) {
		t.Fatalf("expected %q to be in %q", val1, content)
	}
}
