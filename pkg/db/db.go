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

type NewDBOptions struct {
	File    ReadWriteCloserSeeker
	LogFile io.ReadWriter
}

func initializeStorage(storageName string) (ReadWriteCloserSeeker, error) {
	f, err := os.OpenFile("/tmp/gogressdb_"+storageName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return f, nil
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
		f, err := initializeStorage("default")
		if err != nil {
			return nil, err
		}
		db.Tables["default"].Storage = f
	}

	if db.LogFile == nil {
		db.LogFile = os.Stdout
	}

	// load index from file
	// TODO add list of storage files
	storageFiles := map[string]io.ReadSeeker{}
	m, err := BuildIndex(db.LogFile, storageFiles)
	for tableName, idx := range m {
		db.Tables[tableName].KeyOffsets = idx
	}
	if err != nil {
		return nil, err
	}

	return db, nil
}

var InvalidLogLineError = errors.New("invalid log line")

// format is tableName:key:value
// For crash recovery we scan the storage and apply the log
func BuildIndex(walFile io.Reader, storageFiles map[string]io.ReadSeeker) (map[string]map[string]int64, error) {
	finalIndex := map[string]map[string]int64{}
	// Scan storage files
	indexFromStorage := map[string]map[string]int64{}
	for tableName, storage := range storageFiles {
		storageTableIdx, err := BuildIndexFromStorage(storage)
		if err != nil {
			return nil, err
		}

		indexFromStorage[tableName] = storageTableIdx
	}

	// Scan WAL
	indexFromWal, err := BuildIndexFromWAL(walFile, storageFiles)
	if err != nil {
		return nil, err
	}

	for tableName := range indexFromStorage {
		finalIndex[tableName] = indexFromStorage[tableName]
	}

	for tableName := range indexFromWal {
		for k, v := range indexFromWal[tableName] {
			if finalIndex[tableName] == nil {
				finalIndex[tableName] = map[string]int64{}
			}
			finalIndex[tableName][k] = v
		}
	}

	return finalIndex, nil
}

func BuildIndexFromStorage(storage io.ReadSeeker) (map[string]int64, error) {
	idx := map[string]int64{}
	currentOffset := 0
	for {
		content := make([]byte, paddingSize)
		readBytes, err := storage.Read(content)
		if err != nil {
			switch err {
			case io.EOF:
				// End of file reached
				return idx, nil
			default:
				return nil, err
			}
		}
		if readBytes != paddingSize {
			return nil, InvalidLogLineError
		}
		// split on :
		splitted := bytes.Split(content, []byte(":"))
		// process current
		if len(splitted) != 2 {
			return nil, InvalidLogLineError
		}
		key := splitted[0]
		idx[string(key)] = int64(currentOffset)
		currentOffset += paddingSize
	}
}

func BuildIndexFromWAL(walFile io.Reader, storageFiles map[string]io.ReadSeeker) (map[string]map[string]int64, error) {
	idx := map[string]map[string]int64{}
	scanner := bufio.NewScanner(walFile)

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

type CreateTableOptions struct {
	Storage ReadWriteCloserSeeker
}

var CreateTableAlreadyExistsError = errors.New("table already exists")

func (db *DB) CreateTable(tableName string, opts *CreateTableOptions) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	if _, ok := db.Tables[tableName]; ok {
		return CreateTableAlreadyExistsError
	}

	db.Tables[tableName] = &Table{
		KeyOffsets: make(map[string]int64),
	}

	if opts.Storage != nil {
		db.Tables[tableName].Storage = opts.Storage
	} else {
		f, err := initializeStorage(tableName)
		if err != nil {
			return err
		}
		db.Tables[tableName].Storage = f
	}

	return nil
}
