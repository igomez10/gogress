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

// InternalSchemasSchema returns the hardcoded schema for the internal_schemas table.
// Each record stores one table's full schema: id=tablename, schema="col:type:width,..."
func InternalSchemasSchema() Schema {
	return Schema{
		Columns: []Column{
			{Name: "id", Type: ColumnTypeString, Width: 32},
			{Name: "schema", Type: ColumnTypeString, Width: 96},
		},
	}
}

// serializeSchemaString encodes a Schema as "col:type:width,col:type:width,..."
func serializeSchemaString(schema Schema) string {
	parts := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		typeName := "string"
		if col.Type == ColumnTypeInt {
			typeName = "int"
		}
		parts[i] = fmt.Sprintf("%s:%s:%d", col.Name, typeName, col.Width)
	}
	return strings.Join(parts, ",")
}

// deserializeSchemaString decodes "col:type:width,col:type:width,..." into a Schema.
func deserializeSchemaString(s string) (Schema, error) {
	s = strings.TrimRight(s, "\x00")
	if s == "" {
		return Schema{}, fmt.Errorf("empty schema string")
	}
	parts := strings.Split(s, ",")
	columns := make([]Column, 0, len(parts))
	for _, part := range parts {
		fields := strings.SplitN(part, ":", 3)
		if len(fields) != 3 {
			return Schema{}, fmt.Errorf("invalid schema part: %q", part)
		}
		width, err := strconv.Atoi(fields[2])
		if err != nil {
			return Schema{}, fmt.Errorf("invalid width in schema part %q: %w", part, err)
		}
		colType := ColumnTypeString
		if fields[1] == "int" {
			colType = ColumnTypeInt
		}
		columns = append(columns, Column{Name: fields[0], Type: colType, Width: width})
	}
	return Schema{Columns: columns}, nil
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
		Schema:     DefaultSchema(),
	}

	if db.Tables["default"].Storage == nil {
		f, err := initializeStorage("default")
		if err != nil {
			return nil, err
		}
		db.Tables["default"].Storage = f
	}

	// create internal_schemas table
	db.Tables["internal_schemas"] = &Table{
		KeyOffsets: make(map[string]int64),
		Schema:     InternalSchemasSchema(),
	}
	{
		f, err := initializeStorage("internal_schemas")
		if err != nil {
			return nil, err
		}
		db.Tables["internal_schemas"].Storage = f
	}

	if db.LogFile == nil {
		db.LogFile = os.Stdout
	}

	// Discover storage files
	tables := findTables(storagePrefix)
	storageFiles := map[string]io.ReadWriteSeeker{}
	for _, table := range tables {
		splitted := strings.Split(table.Name(), "/")
		tableName := splitted[len(splitted)-1]
		storageFiles[tableName] = table
		if db.Tables[tableName] == nil {
			// Placeholder; schema will be filled from internal_schemas below
			db.Tables[tableName] = &Table{
				KeyOffsets: make(map[string]int64),
				Storage:    table,
				Schema:     DefaultSchema(),
			}
		}
	}

	// Build index for internal_schemas first so we can read it
	isSchema := InternalSchemasSchema()
	if sf, ok := storageFiles["internal_schemas"]; ok {
		isIdx, err := BuildIndexFromStorage(sf, isSchema)
		if err != nil {
			isIdx = map[string]int64{}
		}
		db.Tables["internal_schemas"].KeyOffsets = isIdx
	}

	// Read all records from internal_schemas to reconstruct schemas
	isTbl := db.Tables["internal_schemas"]
	for key := range isTbl.KeyOffsets {
		rec, found, err := isTbl.Get([]byte(key))
		if err != nil || !found {
			continue
		}
		tblName := string(bytes.TrimRight(rec.Columns["id"], "\x00"))
		schemaStr := string(bytes.TrimRight(rec.Columns["schema"], "\x00"))
		schema, err := deserializeSchemaString(schemaStr)
		if err != nil {
			continue
		}
		if tbl, ok := db.Tables[tblName]; ok {
			tbl.Schema = schema
		}
	}

	// Build schemas map for full BuildIndex
	schemas := map[string]Schema{}
	for name, tbl := range db.Tables {
		schemas[name] = tbl.Schema
	}

	m, err := BuildIndex(db.LogFile, storageFiles, schemas)
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

// BuildIndex rebuilds the in-memory index from storage files and WAL.
func BuildIndex(walFile io.Reader, storageFiles map[string]io.ReadWriteSeeker, schemas map[string]Schema) (map[string]map[string]int64, error) {
	finalIndex := map[string]map[string]int64{}
	// Scan storage files
	for tableName, storage := range storageFiles {
		schema := schemas[tableName]
		storageTableIdx, err := BuildIndexFromStorage(storage, schema)
		if err != nil {
			// Incompatible storage file (e.g. old format), truncate and start fresh
			if seeker, ok := storage.(io.WriteSeeker); ok {
				seeker.Seek(0, io.SeekStart)
				if trunc, ok := storage.(interface{ Truncate(int64) error }); ok {
					trunc.Truncate(0)
				}
			}
			finalIndex[tableName] = map[string]int64{}
			continue
		}

		finalIndex[tableName] = storageTableIdx
	}

	// Load WAL
	changeLog, err := ParseWAL(walFile, schemas)
	if err != nil {
		return nil, err
	}

	// Reconcile changes
	if _, err := ReconcileChanges(finalIndex, changeLog, storageFiles, schemas); err != nil {
		return nil, err
	}

	return finalIndex, nil
}

func ReconcileChanges(finalIndex map[string]map[string]int64, changeLog map[string][]Record, storageFiles map[string]io.ReadWriteSeeker, schemas map[string]Schema) (int64, error) {
	var totalChanges int64
	for tableName, recordChanges := range changeLog {
		// Skip WAL entries for tables with no storage file (e.g. deleted tables)
		if _, ok := storageFiles[tableName]; !ok {
			continue
		}
		schema := schemas[tableName]
		if finalIndex[tableName] == nil {
			finalIndex[tableName] = map[string]int64{}
		}
		// traverse the WAL and get the latest value for each key
		// We deduplicate by traversing in reverse
		type latestRecord struct {
			rec Record
		}
		latestByKey := map[string]latestRecord{}
		for i := 0; i < len(recordChanges); i++ {
			currentRec := recordChanges[len(recordChanges)-1-i]
			pk := string(currentRec.Key(schema))
			if _, exists := latestByKey[pk]; !exists {
				latestByKey[pk] = latestRecord{rec: currentRec}
			}
		}

		for _, currentRecordFromWAL := range recordChanges {
			pk := string(currentRecordFromWAL.Key(schema))
			currentTableIndex := finalIndex[tableName]
			recordOffset, exists := currentTableIndex[pk]
			if !exists {
				// key in wal not found in index, add it
				tableFile := storageFiles[tableName]
				offset, err := WriteRecordToFile(tableFile, currentRecordFromWAL, schema)
				if err != nil {
					return 0, err
				}

				finalIndex[tableName][pk] = offset
				totalChanges++
				continue
			}

			// record exists, check if value matches
			tableFile := storageFiles[tableName]
			recordFromStorage, err := ReadRecordFromFile(tableFile, recordOffset, schema)
			if err != nil {
				return 0, err
			}

			storagePK := string(recordFromStorage.Key(schema))
			if storagePK != pk {
				return 0, fmt.Errorf("key mismatch: expected %q, got %q", pk, storagePK)
			}

			// Compare all column values
			if !recordsEqual(recordFromStorage, currentRecordFromWAL, schema) {
				latest := latestByKey[pk].rec
				offset, err := WriteRecordToFile(tableFile, latest, schema)
				if err != nil {
					return 0, err
				}

				finalIndex[tableName][pk] = offset
				totalChanges++
			}
		}
	}
	return totalChanges, nil
}

func recordsEqual(a, b Record, schema Schema) bool {
	for _, col := range schema.Columns {
		if !bytes.Equal(a.Columns[col.Name], b.Columns[col.Name]) {
			return false
		}
	}
	return true
}

func BuildIndexFromStorage(storage io.ReadSeeker, schema Schema) (map[string]int64, error) {
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
				return idx, nil
			default:
				return nil, err
			}
		}
		if readBytes != paddingSize {
			return nil, InvalidLogLineError
		}

		// Extract primary key from the record using schema
		pos := 0
		pkCol := schema.Columns[0]
		pkBytes := bytes.TrimRight(content[pos:pos+pkCol.Width], "\x00")
		if len(pkBytes) == 0 {
			return nil, InvalidLogLineError
		}

		idx[string(pkBytes)] = int64(currentOffset)
		currentOffset += paddingSize
	}
}

// WAL format: tableName:col1val:col2val:...\n
// Column values are positional based on schema order.
func ParseWAL(walFile io.Reader, schemas map[string]Schema) (map[string][]Record, error) {
	changeLog := map[string][]Record{}
	scanner := bufio.NewScanner(walFile)

	for scanner.Scan() {
		line := scanner.Text()
		// First field is table name, rest are column values
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			// Skip incompatible WAL lines (e.g. old format)
			continue
		}

		tableName := parts[0]
		schema, ok := schemas[tableName]
		if !ok {
			// Use default schema if table not known
			schema = DefaultSchema()
		}

		colParts := strings.SplitN(parts[1], ":", len(schema.Columns))
		if len(colParts) != len(schema.Columns) {
			// Skip WAL lines that don't match the current schema
			continue
		}

		rec := Record{Columns: make(map[string][]byte)}
		for i, col := range schema.Columns {
			rec.Columns[col.Name] = []byte(colParts[i])
		}

		if changeLog[tableName] == nil {
			changeLog[tableName] = []Record{}
		}
		changeLog[tableName] = append(changeLog[tableName], rec)
	}

	return changeLog, nil
}

type DB struct {
	LogFile io.ReadWriter

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
	Limit     int
	Offset    int
	FilterCol string // empty = no filter
	FilterVal string
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
		rec, found, err := table.Get([]byte(key))
		if err != nil {
			return nil, err
		}
		if found {
			if scanOptions.FilterCol != "" {
				val := strings.TrimRight(string(rec.Columns[scanOptions.FilterCol]), "\x00")
				if val != scanOptions.FilterVal {
					continue
				}
			}
			records = append(records, rec)
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

func (db *DB) Put(tableName string, colValues map[string][]byte) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	table, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %q not found", tableName)
	}

	hook := func() {
		// write to log: tableName:col1val:col2val:...
		parts := make([]string, 0, len(table.Schema.Columns)+1)
		parts = append(parts, tableName)
		for _, col := range table.Schema.Columns {
			parts = append(parts, string(colValues[col.Name]))
		}
		fmt.Fprintln(db.LogFile, strings.Join(parts, ":"))
	}

	return table.Put(colValues, hook)
}

func (db *DB) Get(tableName string, key []byte) (Record, bool, error) {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()

	table, ok := db.Tables[tableName]
	if !ok {
		return Record{}, false, fmt.Errorf("table %q not found", tableName)
	}

	return table.Get(key)
}

type CreateTableOptions struct {
	Storage ReadWriteCloserSeeker
	Schema  Schema
}

var CreateTableAlreadyExistsError = errors.New("table already exists")

func (db *DB) CreateTable(tableName string, opts *CreateTableOptions) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	if _, ok := db.Tables[tableName]; ok {
		return CreateTableAlreadyExistsError
	}

	schema := DefaultSchema()
	if opts != nil && len(opts.Schema.Columns) > 0 {
		schema = opts.Schema
	}

	db.Tables[tableName] = &Table{
		KeyOffsets: make(map[string]int64),
		Schema:     schema,
	}

	if opts != nil && opts.Storage != nil {
		db.Tables[tableName].Storage = opts.Storage
	} else {
		f, err := initializeStorage(tableName)
		if err != nil {
			return err
		}
		db.Tables[tableName].Storage = f
	}

	// Persist schema to internal_schemas (one record per table)
	if isTbl, ok := db.Tables["internal_schemas"]; ok {
		colValues := map[string][]byte{
			"id":     []byte(tableName),
			"schema": []byte(serializeSchemaString(schema)),
		}
		if err := isTbl.Put(colValues, func() {
			parts := []string{"internal_schemas"}
			for _, c := range isTbl.Schema.Columns {
				parts = append(parts, string(colValues[c.Name]))
			}
			fmt.Fprintln(db.LogFile, strings.Join(parts, ":"))
		}); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) SQL(query string) ([]Record, error) {
	query = strings.TrimSpace(query)
	parts := strings.Fields(query)
	if len(parts) == 0 {
		return nil, nil
	}

	switch strings.ToUpper(parts[0]) {
	case "SELECT":
		if len(parts) < 4 {
			return nil, fmt.Errorf("invalid SELECT query: %q", query)
		}
		table := parts[3]
		opts := ScanOptions{
			Limit:  10,
			Offset: 0,
		}
		if len(parts) >= 5 && strings.ToUpper(parts[4]) == "WHERE" {
			// Support both "col = 'val'" (3 tokens) and "col=val" / "col='val'" (1 token)
			if len(parts) >= 8 && parts[6] == "=" {
				opts.FilterCol = parts[5]
				opts.FilterVal = strings.Trim(parts[7], "'")
			} else if len(parts) >= 6 {
				expr := parts[5]
				if eqIdx := strings.Index(expr, "="); eqIdx > 0 {
					opts.FilterCol = expr[:eqIdx]
					opts.FilterVal = strings.Trim(expr[eqIdx+1:], "'")
				}
			}
		}
		records, err := db.Scan(table, opts)
		if err != nil {
			return nil, err
		}
		return records, nil

	case "CREATE":
		if len(parts) < 3 || strings.ToUpper(parts[1]) != "TABLE" {
			return nil, fmt.Errorf("invalid CREATE query: %q", query)
		}
		tableName := parts[2]
		if err := db.CreateTable(tableName, nil); err != nil {
			return nil, err
		}
		return nil, nil

	default:
		panic("unsupported query")
	}
}

var DeleteDefaultTableError = errors.New("cannot delete default table")

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

	if table.Storage != nil {
		if err := table.Storage.Close(); err != nil {
			return err
		}
		if name := table.Storage.Name(); name != "" {
			if err := os.Remove(name); err != nil {
				return err
			}
		}
	}

	// Remove schema record from internal_schemas by writing a tombstone
	if isTbl, ok := db.Tables["internal_schemas"]; ok {
		colValues := map[string][]byte{
			"id":     []byte(tableName),
			"schema": []byte(""),
		}
		_ = isTbl.Put(colValues, func() {
			parts := []string{"internal_schemas", tableName, ""}
			fmt.Fprintln(db.LogFile, strings.Join(parts, ":"))
		})
		delete(isTbl.KeyOffsets, tableName)
	}

	delete(db.Tables, tableName)
	return nil
}
