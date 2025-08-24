package db

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type NewDBOptions struct {
	File    ReadWriteCloserSeeker
	LogFile io.ReadWriter
}

var storagePrefix = "/tmp/gogress/"

func initializeStorage(storageName string) (ReadWriteCloserSeeker, error) {
	// make sure prefix exists
	if err := os.MkdirAll(storagePrefix, 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(storagePrefix+storageName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
	tables := findTables(storagePrefix)
	storageFiles := map[string]io.ReadWriteSeeker{}
	for _, table := range tables {
		splitted := strings.Split(table.Name(), "/")
		tableName := splitted[len(splitted)-1]
		storageFiles[tableName] = table
		if db.Tables[tableName] == nil {
			db.Tables[tableName] = &Table{
				KeyOffsets: make(map[string]int64),
				Storage:    table,
			}
		}
	}
	m, err := BuildIndex(db.LogFile, storageFiles)
	if err != nil {
		return nil, err
	}
	for tableName, idx := range m {
		db.Tables[tableName].KeyOffsets = idx
	}

	return db, nil
}

// find tables in dir that match prefix and return them, these are the tables
func findTables(storagePrefix string) []*os.File {
	var tables []*os.File
	files, err := os.ReadDir(storagePrefix)
	if err != nil {
		return nil
	}
	for _, file := range files {
		if file.IsDir() || file.Name() == ".DS_Store" {
			continue
		}
		f, err := os.OpenFile(storagePrefix+file.Name(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil
		}
		tables = append(tables, f)
	}
	return tables
}

var InvalidLogLineError = errors.New("invalid log line")

// format is tableName:key:value
// For crash recovery we scan the storage and apply the log
func BuildIndex(walFile io.Reader, storageFiles map[string]io.ReadWriteSeeker) (map[string]map[string]int64, error) {
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

	// Load WAL
	changeLog, err := ParseWAL(walFile, storageFiles)
	if err != nil {
		return nil, err
	}

	// Reconcile changes
	if _, err := ReconcileChanges(finalIndex, changeLog, storageFiles); err != nil {
		return nil, err
	}

	return finalIndex, nil
}

func ReconcileChanges(finalIndex map[string]map[string]int64, changeLog map[string]map[string]string, storageFiles map[string]io.ReadWriteSeeker) (int64, error) {
	var totalChanges int64
	for tableName, tableChanges := range changeLog {
		if finalIndex[tableName] == nil {
			finalIndex[tableName] = map[string]int64{}
		}

		// check all changes and compare current stored version with new version
		for key, value := range tableChanges {
			offset, ok := finalIndex[tableName][key]
			if !ok {
				// key not found in index, add it
				newrecord := Record{
					Key:   []byte(key),
					Value: []byte(value),
				}
				tableFile := storageFiles[tableName]
				offset, err := WriteRecordToFile(tableFile, newrecord)
				if err != nil {
					return 0, err
				}

				// update index in finalIndex
				finalIndex[tableName][key] = offset
				totalChanges++
				continue
			}

			record, err := ReadRecordFromFile(storageFiles[tableName], offset)
			if err != nil {
				return 0, err
			}
			if string(record.Key) != key {
				return 0, fmt.Errorf("key mismatch: expected %q, got %q", key, record.Key)
			}
			if string(record.Value) != value {
				// found key with stale value, update with new value
				newrecord := Record{
					Key:   []byte(key),
					Value: []byte(value),
				}
				offset, err := WriteRecordToFile(storageFiles[tableName], newrecord)
				if err != nil {
					return 0, err
				}

				// update index in finalIndex
				finalIndex[tableName][key] = offset

				totalChanges++
			}
		}
	}
	return totalChanges, nil
}

func BuildIndexFromStorage(storage io.ReadSeeker) (map[string]int64, error) {
	if storage == nil {
		return map[string]int64{}, nil
	}

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

func ParseWAL(walFile io.Reader, storageFiles map[string]io.ReadWriteSeeker) (map[string]map[string]string, error) {
	changeLog := map[string]map[string]string{}
	scanner := bufio.NewScanner(walFile)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			return nil, InvalidLogLineError
		}
		tableName := parts[0]
		key := parts[1]
		value := parts[2]
		if changeLog[tableName] == nil {
			changeLog[tableName] = map[string]string{}
		}
		changeLog[tableName][key] = value
	}

	return changeLog, nil
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

	hook := func() {
		// write to log for crash recovery
		fmt.Fprintln(db.LogFile, string(tableName)+":"+string(key)+":"+string(val))
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
