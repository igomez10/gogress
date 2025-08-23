package db

import "io"

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
