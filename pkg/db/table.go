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
