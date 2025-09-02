package storage

import (
	"errors"
	"io"
)

// Used for testing

var _ io.ReadWriteSeeker = (*InMemoryStorage)(nil)

type InMemoryStorage struct {
	Data   []byte
	Cursor int64
}

// Read implements io.ReadWriteSeeker.
func (i *InMemoryStorage) Read(p []byte) (n int, err error) {
	if i.Cursor >= int64(len(i.Data)) {
		return 0, io.EOF
	}
	n = copy(p, i.Data[i.Cursor:])
	i.Cursor += int64(n)
	return
}

// Seek implements io.ReadWriteSeeker.
func (i *InMemoryStorage) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, errors.New("negative seek offset")
		}
		i.Cursor = offset
	case io.SeekCurrent:
		if offset < 0 && -offset > i.Cursor {
			return 0, errors.New("negative seek offset out of bounds")
		}
		i.Cursor += offset
	case io.SeekEnd:
		if offset < 0 && -offset > int64(len(i.Data)) {
			return 0, errors.New("negative seek offset out of bounds")
		}
		i.Cursor = int64(len(i.Data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if i.Cursor < 0 {
		i.Cursor = 0
	}
	return i.Cursor, nil
}

// Write implements io.ReadWriteSeeker.
func (i *InMemoryStorage) Write(p []byte) (n int, err error) {
	// here we ensure the underlying slice is large enough
	if i.Cursor+int64(len(p)) > int64(len(i.Data)) {
		newSlice := make([]byte, i.Cursor+int64(len(p)))
		for idx := range i.Data {
			newSlice[idx] = i.Data[idx]
		}
		i.Data = newSlice
	}

	counter := 0
	for idx := range p {
		i.Data[i.Cursor] = p[idx]
		i.Cursor++
		counter++
	}
	return counter, nil
}
