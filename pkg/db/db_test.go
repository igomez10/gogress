package db

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/igomez10/gogress/pkg/storage"
)

func defaultSchemas() map[string]Schema {
	return map[string]Schema{
		"default": DefaultSchema(),
	}
}

func Test_BuildIndex(t *testing.T) {
	type args struct {
		idx      map[string]int64
		walIndex io.Reader
		storage  map[string]io.ReadWriteSeeker
	}
	tests := []struct {
		name        string
		args        args
		want        map[string]map[string]int64
		expectedErr error
	}{
		{
			name: "valid log",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("default:key1:1\ndefault:key2:2\n"),
				storage: map[string]io.ReadWriteSeeker{
					"default": &storage.InMemoryStorage{},
				},
			},
			want: map[string]map[string]int64{
				"default": {
					"key1": paddingSize * 0,
					"key2": paddingSize * 1,
				},
			},
		},
		{
			name: "invalid log lines are skipped",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("invalid\n"),
				storage: map[string]io.ReadWriteSeeker{
					"default": &storage.InMemoryStorage{},
				},
			},
			want: map[string]map[string]int64{},
		},
		{
			name: "empty log",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader(""),
				storage: map[string]io.ReadWriteSeeker{
					"default": &storage.InMemoryStorage{},
				},
			},
			want: map[string]map[string]int64{},
		},
		{
			name: "wal and storage are same",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("default:key1:value1\n"),
				storage: func() map[string]io.ReadWriteSeeker {
					res := map[string]io.ReadWriteSeeker{}
					f := &storage.InMemoryStorage{}
					res["default"] = f
					return res
				}(),
			},
			want: map[string]map[string]int64{
				"default": {
					"key1": 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offsets, err := BuildIndex(tt.args.walIndex, tt.args.storage, defaultSchemas())
			if (err != nil && !errors.Is(err, tt.expectedErr)) || (err == nil && tt.expectedErr != nil) {
				t.Errorf("BuildIndex() error = %v, want %v", err, tt.expectedErr)
			}

			for tableName, idx := range tt.want {
				for k, v := range idx {
					if got := offsets[tableName][k]; got != v {
						t.Errorf("BuildIndex() = %v, want %v", got, v)
					}
				}
			}
		})
	}
}

func TestDB_CreateTable(t *testing.T) {
	t.Run("create new table writes schema to internal_schemas", func(t *testing.T) {
		isTbl := &Table{
			KeyOffsets: make(map[string]int64),
			Storage:    &MockReadWriteCloserSeeker{},
			Schema:     InternalSchemasSchema(),
		}
		db := &DB{
			LogFile: &bytes.Buffer{},
			Tables: map[string]*Table{
				"internal_schemas": isTbl,
			},
		}
		schema := Schema{Columns: []Column{
			{Name: "id", Type: ColumnTypeString, Width: 32},
			{Name: "name", Type: ColumnTypeString, Width: 32},
			{Name: "age", Type: ColumnTypeInt, Width: 8},
		}}
		err := db.CreateTable("students", &CreateTableOptions{
			Storage: &MockReadWriteCloserSeeker{},
			Schema:  schema,
		})
		if err != nil {
			t.Fatalf("CreateTable() error = %v", err)
		}

		// Verify 1 record in internal_schemas (one per table)
		if len(isTbl.KeyOffsets) != 1 {
			t.Fatalf("expected 1 schema record, got %d", len(isTbl.KeyOffsets))
		}
		if _, ok := isTbl.KeyOffsets["students"]; !ok {
			t.Errorf("missing schema record for %q", "students")
		}
		// Verify we can deserialize the stored schema
		rec, found, err := isTbl.Get([]byte("students"))
		if err != nil || !found {
			t.Fatalf("failed to get schema record: err=%v found=%v", err, found)
		}
		recovered, err := deserializeSchemaString(string(rec.Columns["schema"]))
		if err != nil {
			t.Fatalf("deserializeSchemaString() error = %v", err)
		}
		if len(recovered.Columns) != 3 {
			t.Fatalf("expected 3 columns, got %d", len(recovered.Columns))
		}
		if recovered.TotalWidth() != schema.TotalWidth() {
			t.Errorf("total width mismatch: got %d, want %d", recovered.TotalWidth(), schema.TotalWidth())
		}
	})

	t.Run("create existing table", func(t *testing.T) {
		db := &DB{
			Tables: map[string]*Table{"existing_table": {}},
		}
		if err := db.CreateTable("existing_table", &CreateTableOptions{}); err != CreateTableAlreadyExistsError {
			t.Errorf("DB.CreateTable() error = %v, expectedErr %v", err, CreateTableAlreadyExistsError)
		}
	})
}

func TestDB_SchemaRecovery(t *testing.T) {
	// Create a DB with internal_schemas, create a table, then simulate restart
	// by building a new DB from the same storage.
	isStorage := &MockReadWriteCloserSeeker{}
	coursesStorage := &MockReadWriteCloserSeeker{}

	isTbl := &Table{
		KeyOffsets: make(map[string]int64),
		Storage:    isStorage,
		Schema:     InternalSchemasSchema(),
	}
	db1 := &DB{
		LogFile: &bytes.Buffer{},
		Tables: map[string]*Table{
			"default":          {KeyOffsets: make(map[string]int64), Storage: &MockReadWriteCloserSeeker{}, Schema: DefaultSchema()},
			"internal_schemas": isTbl,
		},
	}

	schema := Schema{Columns: []Column{
		{Name: "id", Type: ColumnTypeString, Width: 32},
		{Name: "name", Type: ColumnTypeString, Width: 32},
		{Name: "createdat", Type: ColumnTypeInt, Width: 8},
	}}
	err := db1.CreateTable("courses", &CreateTableOptions{
		Storage: coursesStorage,
		Schema:  schema,
	})
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Now simulate recovery: read internal_schemas storage to reconstruct the schema
	isStorage.Seek(0, io.SeekStart)
	isIdx, err := BuildIndexFromStorage(isStorage, InternalSchemasSchema())
	if err != nil {
		t.Fatalf("BuildIndexFromStorage() error = %v", err)
	}

	// Reconstruct schema from the single record
	recoveredTbl := &Table{
		KeyOffsets: isIdx,
		Storage:    isStorage,
		Schema:     InternalSchemasSchema(),
	}
	rec, found, err := recoveredTbl.Get([]byte("courses"))
	if err != nil || !found {
		t.Fatalf("failed to get schema record for courses: err=%v found=%v", err, found)
	}
	recoveredSchema, err := deserializeSchemaString(string(rec.Columns["schema"]))
	if err != nil {
		t.Fatalf("deserializeSchemaString() error = %v", err)
	}
	if len(recoveredSchema.Columns) != 3 {
		t.Fatalf("expected 3 columns recovered, got %d", len(recoveredSchema.Columns))
	}
	if recoveredSchema.TotalWidth() != schema.TotalWidth() {
		t.Errorf("total width mismatch: got %d, want %d", recoveredSchema.TotalWidth(), schema.TotalWidth())
	}
	// Verify column order is preserved
	for i, col := range schema.Columns {
		if recoveredSchema.Columns[i].Name != col.Name {
			t.Errorf("column %d name: got %q, want %q", i, recoveredSchema.Columns[i].Name, col.Name)
		}
	}
}

func TestBuildIndexFromStorage(t *testing.T) {
	schema := DefaultSchema()

	type args struct {
		storage io.ReadSeeker
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]int64
		wantErr bool
	}{
		{
			name: "valid storage",
			args: args{
				storage: bytes.NewReader(makeStorageData("key1", "value1", "somelongkey", "value2", "short", "k3")),
			},
			want: map[string]int64{
				"key1":        0,
				"somelongkey": paddingSize * 1,
				"short":       paddingSize * 2,
			},
		},
		{
			name: "invalid line - too short",
			args: args{
				storage: strings.NewReader("invalid\nlog\n"),
			},
			wantErr: true,
		},
		{
			name: "empty primary key",
			args: args{
				storage: func() io.ReadSeeker {
					// Create a 128-byte record where the first 16 bytes (pk column) are all zeros
					data := make([]byte, paddingSize)
					return bytes.NewReader(data)
				}(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildIndexFromStorage(tt.args.storage, schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildIndexFromStorage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BuildIndexFromStorage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileChanges(t *testing.T) {
	schema := DefaultSchema()
	schemas := defaultSchemas()

	type args struct {
		finalIndex   map[string]map[string]int64
		changeLog    map[string][]Record
		storageFiles map[string]io.ReadWriteSeeker
	}
	tests := []struct {
		name                 string
		args                 args
		expectedTotalChanges int64
		expectedFinalIndex   map[string]map[string]int64
		wantErr              bool
	}{
		{
			name: "nothing to reconcile",
			args: args{
				finalIndex: map[string]map[string]int64{
					"default": {"key1": 0},
				},
				changeLog: map[string][]Record{
					"default": {makeRecord("key1", "value1")},
				},
				storageFiles: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f, _ := os.CreateTemp("", "")
						f.Write(makeStorageData("key1", "value1"))
						return f
					}(),
				},
			},
			expectedFinalIndex: map[string]map[string]int64{
				"default": {"key1": 0},
			},
			expectedTotalChanges: 0,
		},
		{
			name: "reconcile 1 change, same key different value",
			args: args{
				finalIndex: map[string]map[string]int64{
					"default": {"key1": 0},
				},
				changeLog: map[string][]Record{
					"default": {makeRecord("key1", "newvalue1")},
				},
				storageFiles: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f, _ := os.CreateTemp("", "")
						f.Write(makeStorageData("key1", "value1"))
						return f
					}(),
				},
			},
			expectedFinalIndex: map[string]map[string]int64{
				"default": {"key1": paddingSize * 1},
			},
			expectedTotalChanges: 1,
		},
		{
			name: "reconcile 1 change, new key",
			args: args{
				finalIndex: map[string]map[string]int64{
					"default": {"key1": 0},
				},
				changeLog: map[string][]Record{
					"default": {
						makeRecord("key1", "newvalue1"),
						makeRecord("key2", "newvalue2"),
					},
				},
				storageFiles: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f := &storage.InMemoryStorage{}
						f.Write(makeStorageData("key1", "value1"))
						return f
					}(),
				},
			},
			expectedFinalIndex: map[string]map[string]int64{
				"default": {
					"key1": paddingSize * 1,
					"key2": paddingSize * 2,
				},
			},
			expectedTotalChanges: 2,
		},
	}
	_ = schema
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			totalChanges, err := ReconcileChanges(tt.args.finalIndex, tt.args.changeLog, tt.args.storageFiles, schemas)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ReconcileChanges() error = %v, wantErr %v", err, tt.wantErr)
			}
			if totalChanges != tt.expectedTotalChanges {
				t.Fatalf("ReconcileChanges() = %v, want %v", totalChanges, tt.expectedTotalChanges)
			}

			if !reflect.DeepEqual(tt.args.finalIndex, tt.expectedFinalIndex) {
				t.Fatalf("ReconcileChanges() index = %v, want %v", tt.args.finalIndex, tt.expectedFinalIndex)
			}
		})
	}
}

func TestFindTables(t *testing.T) {
	prefix := "tmp/gogress_test/"
	if err := os.MkdirAll(prefix, 0755); err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	f1, err := os.CreateTemp(prefix, "file1.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	f2, err := os.CreateTemp(prefix, "file2.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	f3, err := os.CreateTemp(prefix, "file3.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	defer os.Remove(f1.Name())
	defer os.Remove(f2.Name())
	defer os.Remove(f3.Name())

	arr := findTables(prefix)

	if len(arr) != 3 {
		t.Errorf("findTables() = %v, want 3", arr)
	}
}

func TestDB_DeleteTable_DefaultTableError(t *testing.T) {
	db := &DB{
		Tables: map[string]*Table{
			"default": {},
		},
	}

	if err := db.DeleteTable("default"); !errors.Is(err, DeleteDefaultTableError) {
		t.Fatalf("DeleteTable(default) error = %v, want %v", err, DeleteDefaultTableError)
	}
}

func TestDB_DeleteTable_RemovesFileAndEntry(t *testing.T) {
	f, err := os.CreateTemp("", "table-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := f.Name()

	db := &DB{
		Tables: map[string]*Table{
			"foo":     {KeyOffsets: make(map[string]int64), Storage: f},
			"default": {},
		},
	}

	if err := db.DeleteTable("foo"); err != nil {
		t.Fatalf("DeleteTable(foo) error = %v, want nil", err)
	}

	if _, ok := db.Tables["foo"]; ok {
		t.Fatalf("table entry not removed from DB map")
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected storage file to be removed, got err = %v", err)
	}
}

func TestDB_DeleteTable_RemovesSchemaRecord(t *testing.T) {
	isStorage := &MockReadWriteCloserSeeker{}
	isTbl := &Table{
		KeyOffsets: make(map[string]int64),
		Storage:    isStorage,
		Schema:     InternalSchemasSchema(),
	}
	db := &DB{
		LogFile: &bytes.Buffer{},
		Tables: map[string]*Table{
			"default":          {KeyOffsets: make(map[string]int64), Storage: &MockReadWriteCloserSeeker{}, Schema: DefaultSchema()},
			"internal_schemas": isTbl,
		},
	}

	// Create a table so its schema is persisted
	fooStorage, _ := os.CreateTemp("", "foo-*")
	fooPath := fooStorage.Name()
	schema := Schema{Columns: []Column{
		{Name: "id", Type: ColumnTypeString, Width: 32},
		{Name: "val", Type: ColumnTypeString, Width: 32},
	}}
	if err := db.CreateTable("foo", &CreateTableOptions{Storage: fooStorage, Schema: schema}); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	if _, ok := isTbl.KeyOffsets["foo"]; !ok {
		t.Fatalf("expected schema record for foo after create")
	}

	// Delete the table
	if err := db.DeleteTable("foo"); err != nil {
		t.Fatalf("DeleteTable() error = %v", err)
	}

	// Schema record should be removed from in-memory index
	if _, ok := isTbl.KeyOffsets["foo"]; ok {
		t.Fatalf("schema record for foo should be removed after delete")
	}

	// Simulate recovery: rebuild index from internal_schemas storage
	// The tombstone should cause the schema to be skipped
	isStorage.Seek(0, io.SeekStart)
	isIdx, err := BuildIndexFromStorage(isStorage, InternalSchemasSchema())
	if err != nil {
		t.Fatalf("BuildIndexFromStorage() error = %v", err)
	}
	recoveredTbl := &Table{
		KeyOffsets: isIdx,
		Storage:    isStorage,
		Schema:     InternalSchemasSchema(),
	}
	// The latest record for "foo" should have an empty schema (tombstone)
	if _, ok := isIdx["foo"]; ok {
		rec, found, err := recoveredTbl.Get([]byte("foo"))
		if err != nil || !found {
			t.Fatalf("failed to read tombstone record")
		}
		schemaStr := string(bytes.TrimRight(rec.Columns["schema"], "\x00"))
		if schemaStr != "" {
			t.Fatalf("expected empty schema (tombstone), got %q", schemaStr)
		}
		// deserializeSchemaString should fail on empty string
		if _, err := deserializeSchemaString(schemaStr); err == nil {
			t.Fatalf("expected error deserializing empty schema")
		}
	}

	os.Remove(fooPath)
}

func TestDB_DeleteTable_TableNotFound(t *testing.T) {
	db := &DB{Tables: map[string]*Table{"default": {}}}
	if err := db.DeleteTable("missing"); err == nil {
		t.Fatalf("DeleteTable(missing) expected error, got nil")
	}
}

func TestParseWAL(t *testing.T) {
	schemas := defaultSchemas()

	tests := []struct {
		name    string
		walFile io.Reader
		want    map[string][]Record
		wantErr bool
	}{
		{
			name:    "valid WAL with two entries",
			walFile: strings.NewReader("default:key1:val1\ndefault:key2:val2\n"),
			want: map[string][]Record{
				"default": {
					makeRecord("key1", "val1"),
					makeRecord("key2", "val2"),
				},
			},
		},
		{
			name:    "empty WAL",
			walFile: strings.NewReader(""),
			want:    map[string][]Record{},
		},
		{
			name:    "invalid WAL line is skipped",
			walFile: strings.NewReader("invalid\n"),
			want:    map[string][]Record{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseWAL(tt.walFile, schemas)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseWAL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseWAL() = %v, want %v", got, tt.want)
			}
		})
	}
}
