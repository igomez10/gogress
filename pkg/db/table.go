package db

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

type Table struct {
	Mutex sync.RWMutex

	KeyOffsets map[string]int64
	Storage    ReadWriteCloserSeeker
}

const paddingSize = 32

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

	record, err := ReadRecordFromFile(tb.Storage, off)
	if err != nil {
		return nil, false, err
	}

	return record.Value, true, nil
}

type Record struct {
	Key   []byte
	Value []byte
}

func ReadRecordFromFile(f io.ReadSeeker, offset int64) (Record, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return Record{}, err
	}

	reader := io.LimitReader(f, paddingSize)
	buf := make([]byte, paddingSize)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return Record{}, err
	}
	// trim null bytes
	buf = bytes.TrimRight(buf, "\x00")
	// split by first colon
	parts := bytes.SplitN(buf, []byte(":"), 2)
	if len(parts) != 2 {
		return Record{}, fmt.Errorf("invalid record format")
	}
	return Record{
		Key:   parts[0],
		Value: parts[1],
	}, nil
}

func WriteRecordToFile(f io.WriteSeeker, rec Record) (int64, error) {
	if len(rec.Key)+len(rec.Value)+1 > paddingSize {
		return 0, fmt.Errorf("record too large")
	}
	data := make([]byte, 0, paddingSize)
	data = append(data, rec.Key...)
	data = append(data, []byte(":")...)
	data = append(data, rec.Value...)

	// fill remaining space with null bytes
	for i := len(rec.Key) + len(rec.Value) + 1; i < paddingSize; i++ {
		data = append(data, 0)
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
