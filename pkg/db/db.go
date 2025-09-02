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
	for tableName, storage := range storageFiles {
		storageTableIdx, err := BuildIndexFromStorage(storage)
		if err != nil {
			return nil, err
		}

		finalIndex[tableName] = storageTableIdx
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

func ReconcileChanges(finalIndex map[string]map[string]int64, changeLog map[string][]Record, storageFiles map[string]io.ReadWriteSeeker) (int64, error) {
	var totalChanges int64
	for tableName, recordChanges := range changeLog {
		if finalIndex[tableName] == nil {
			finalIndex[tableName] = map[string]int64{}
		}
		// traverse the WAL and get the latest value for each key in it
		// We can have the case where a key is multiple times in the WAL but
		// it was only written once to the storage. In that case we only want to store the last
		// value, not the intermediate steps. For this deduplication step we will traverse the changes
		// for a given table **in reverse** and only store the first value. We will then traverse the changes
		// in order but only write to storage if the value matches the last version
		latestKeyValue := map[string]string{}
		for i := 0; i < len(recordChanges); i++ {
			currentKey := recordChanges[len(recordChanges)-1-i]
			if _, exists := latestKeyValue[string(currentKey.Key)]; !exists {
				latestKeyValue[string(currentKey.Key)] = string(currentKey.Value)
			}
		}

		// check all changes and compare current stored version with new version
		for _, currentRecordFromWAL := range recordChanges {
			currentTableIndex := finalIndex[tableName]
			recordOffset, exists := currentTableIndex[string(currentRecordFromWAL.Key)]
			if !exists {
				// key in wal not found in index, add it to index and to storage
				tableFile := storageFiles[tableName]
				// write new record to storage
				offset, err := WriteRecordToFile(tableFile, currentRecordFromWAL)
				if err != nil {
					return 0, err
				}

				// add record to index
				finalIndex[tableName][string(currentRecordFromWAL.Key)] = offset
				totalChanges++
				continue
			}

			// record exists in index, lets see if the record stored in storage matches the last
			// version in WAL
			tableFile := storageFiles[tableName]
			recordFromStorage, err := ReadRecordFromFile(tableFile, recordOffset)
			if err != nil {
				return 0, err
			}

			// validate the record in the offset matches the expected record by checking key
			if string(recordFromStorage.Key) != string(currentRecordFromWAL.Key) {
				return 0, fmt.Errorf("key mismatch: expected %q, got %q", string(currentRecordFromWAL.Key), string(recordFromStorage.Key))
			}

			// check if the value in storage matches the value in WAL
			if string(recordFromStorage.Value) != string(currentRecordFromWAL.Value) {
				// found key with stale value, update with new value
				r := Record{
					Key:   recordFromStorage.Key,
					Value: []byte(latestKeyValue[string(recordFromStorage.Key)]),
				}
				offset, err := WriteRecordToFile(tableFile, r)
				if err != nil {
					return 0, err
				}

				// update index in finalIndex
				finalIndex[tableName][string(currentRecordFromWAL.Key)] = offset

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

// func ParseWAL(walFile io.Reader, storageFiles map[string]io.ReadWriteSeeker) (map[string]map[string]string, error) {
func ParseWAL(walFile io.Reader, storageFiles map[string]io.ReadWriteSeeker) (map[string][]Record, error) {
	changeLog := map[string][]Record{}
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
			changeLog[tableName] = []Record{}
		}

		changeLog[tableName] = append(changeLog[tableName], Record{
			Key:   []byte(key),
			Value: []byte(value),
		})
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

func (db *DB) Close() error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	for _, table := range db.Tables {
		if err := table.Storage.Close(); err != nil {
			return err
		}
	}

	return nil
}

type ScanOptions struct {
	Limit  int
	Offset int
}

func (db *DB) Scan(tableName string, scanOptions ScanOptions) ([]Record, error) {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()

	table, ok := db.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %q not found", tableName)
	}

	var records []Record
	for key := range table.KeyOffsets {
		val, found, err := table.Get([]byte(key))
		if err != nil {
			return nil, err
		}
		if found {
			records = append(records, Record{
				Key:   []byte(key),
				Value: val,
			})
		}
	}
	return records, nil
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

func (db *DB) SQL(query string) ([]Record, error) {
	// For now, just print the query
	query = strings.TrimSpace(query)
	parts := strings.Fields(query)
	if len(parts) == 0 {
		return nil, nil
	}

	switch parts[0] {
	case "select", "SELECT":
		// Handle SELECT queries
		// columns := parts[1]
		table := parts[3]
		records, err := db.Scan(table, ScanOptions{
			Limit:  10,
			Offset: 0,
		})
		if err != nil {
			return nil, err
		}
		return records, nil

	// case "INSERT":
	// 	// Handle INSERT queries
	// case "UPDATE":
	// 	// Handle UPDATE queries
	// case "DELETE":
	// 	// Handle DELETE queries
	default:
		panic("unsupported query")
	}
}

var DeleteDefaultTableError = errors.New("cannot delete default table")

// DeleteTable removes a table from the DB, closing and deleting its storage file.
func (db *DB) DeleteTable(tableName string) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	if tableName == "default" {
		return DeleteDefaultTableError
	}

	table, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %q not found", tableName)
	}

	// Attempt to close the storage
	if table.Storage != nil {
		if err := table.Storage.Close(); err != nil {
			return err
		}
		// Try to delete the underlying file; ignore errors (e.g., mock storages)
		if name := table.Storage.Name(); name != "" {
			if err := os.Remove(name); err != nil {
				return err
			}
		}
	}

	delete(db.Tables, tableName)
	return nil
}
