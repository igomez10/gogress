package db

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/igomez10/gogress/pkg/storage"
)

func testSchema() Schema {
	return DefaultSchema()
}

func makeRecord(key, value string) Record {
	return Record{
		Columns: map[string][]byte{
			"key":   []byte(key),
			"value": []byte(value),
		},
	}
}

func makeStorageData(pairs ...string) []byte {
	schema := testSchema()
	var buf bytes.Buffer
	for i := 0; i < len(pairs); i += 2 {
		rec := makeRecord(pairs[i], pairs[i+1])
		data, _ := serializeRecord(schema, rec.Columns)
		buf.Write(data)
	}
	return buf.Bytes()
}

func TestReadRecordFromFile(t *testing.T) {
	schema := testSchema()

	type args struct {
		f      io.ReadSeeker
		offset int64
	}
	tests := []struct {
		name    string
		args    args
		want    Record
		wantErr bool
	}{
		{
			name: "valid record",
			args: args{
				f:      bytes.NewReader(makeStorageData("key1", "value1")),
				offset: 0,
			},
			want:    makeRecord("key1", "value1"),
			wantErr: false,
		},
		{
			name: "invalid record - too short",
			args: args{
				f:      bytes.NewReader([]byte("short")),
				offset: 0,
			},
			want:    Record{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadRecordFromFile(tt.args.f, tt.args.offset, schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadRecordFromFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadRecordFromFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteRecordToFile(t *testing.T) {
	schema := testSchema()

	type args struct {
		initialFile io.ReadWriteSeeker
		newRecord   Record
	}
	tests := []struct {
		name         string
		args         args
		expectedFile string
		wantErr      bool
	}{
		{
			name: "valid record in empty file",
			args: args{
				newRecord:   makeRecord("key1", "value1"),
				initialFile: func() io.ReadWriteSeeker { f, _ := os.CreateTemp("", ""); return f }(),
			},
			expectedFile: string(makeStorageData("key1", "value1")),
			wantErr:      false,
		},
		{
			name: "valid record in non-empty file",
			args: args{
				newRecord: makeRecord("key2", "value2"),
				initialFile: func() io.ReadWriteSeeker {
					f, _ := os.CreateTemp("", "")
					f.Write(makeStorageData("key1", "value1"))
					return f
				}(),
			},
			expectedFile: string(makeStorageData("key1", "value1")) + string(makeStorageData("key2", "value2")),
			wantErr:      false,
		},
		{
			name: "valid record in non-empty file with same existing record",
			args: args{
				newRecord: makeRecord("key1", "updatedvalue"),
				initialFile: func() io.ReadWriteSeeker {
					f, _ := os.CreateTemp("", "")
					f.Write(makeStorageData("key1", "value1"))
					return f
				}(),
			},
			expectedFile: string(makeStorageData("key1", "value1")) + string(makeStorageData("key1", "updatedvalue")),
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := WriteRecordToFile(tt.args.initialFile, tt.args.newRecord, schema); (err != nil) != tt.wantErr {
				t.Errorf("WriteRecordToFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.args.initialFile.Seek(0, io.SeekStart)
			content, err := io.ReadAll(tt.args.initialFile)
			if err != nil {
				t.Errorf("WriteRecordToFile() read error = %v", err)
				return
			}
			if gotF := string(content); gotF != tt.expectedFile {
				t.Errorf("WriteRecordToFile() = %q, want %q", gotF, tt.expectedFile)
			}
		})
	}
}

func TestWriteRecordToMock(t *testing.T) {
	schema := testSchema()

	tests := []struct {
		name         string
		initialFile  io.ReadWriteSeeker
		newRecord    Record
		expectedFile string
		wantErr      bool
	}{
		{
			name:         "valid record in empty file",
			newRecord:    makeRecord("key1", "value1"),
			initialFile:  &storage.InMemoryStorage{Data: []byte{}},
			expectedFile: string(makeStorageData("key1", "value1")),
			wantErr:      false,
		},
		{
			name:      "valid record in non-empty file",
			newRecord: makeRecord("key2", "value2"),
			initialFile: func() io.ReadWriteSeeker {
				f := &storage.InMemoryStorage{}
				f.Write(makeStorageData("key1", "value1"))
				return f
			}(),
			expectedFile: string(makeStorageData("key1", "value1")) + string(makeStorageData("key2", "value2")),
			wantErr:      false,
		},
		{
			name:      "valid record in non-empty file with same existing record",
			newRecord: makeRecord("key1", "updatedvalue"),
			initialFile: func() io.ReadWriteSeeker {
				f := &storage.InMemoryStorage{}
				f.Write(makeStorageData("key1", "value1"))
				return f
			}(),
			expectedFile: string(makeStorageData("key1", "value1")) + string(makeStorageData("key1", "updatedvalue")),
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := WriteRecordToFile(tt.initialFile, tt.newRecord, schema); (err != nil) != tt.wantErr {
				t.Errorf("WriteRecordToFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.initialFile.Seek(0, io.SeekStart)
			content, err := io.ReadAll(tt.initialFile)
			if err != nil {
				t.Errorf("WriteRecordToFile() read error = %v", err)
				return
			}
			if gotF := string(content); gotF != tt.expectedFile {
				t.Errorf("WriteRecordToFile() = %q, want %q", gotF, tt.expectedFile)
			}
		})
	}
}

func TestMultiColumnSchema(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: ColumnTypeString, Width: 32},
			{Name: "name", Type: ColumnTypeString, Width: 32},
			{Name: "age", Type: ColumnTypeInt, Width: 8},
		},
	}

	rec := Record{
		Columns: map[string][]byte{
			"id":   []byte("1"),
			"name": []byte("alice"),
			"age":  []byte("30"),
		},
	}

	f := &storage.InMemoryStorage{Data: []byte{}}
	off, err := WriteRecordToFile(f, rec, schema)
	if err != nil {
		t.Fatalf("WriteRecordToFile() error = %v", err)
	}
	if off != 0 {
		t.Fatalf("expected offset 0, got %d", off)
	}

	got, err := ReadRecordFromFile(f, 0, schema)
	if err != nil {
		t.Fatalf("ReadRecordFromFile() error = %v", err)
	}

	if string(got.Columns["id"]) != "1" {
		t.Errorf("id = %q, want %q", got.Columns["id"], "1")
	}
	if string(got.Columns["name"]) != "alice" {
		t.Errorf("name = %q, want %q", got.Columns["name"], "alice")
	}
	if string(got.Columns["age"]) != "30" {
		t.Errorf("age = %q, want %q", got.Columns["age"], "30")
	}
}

func TestColumnOverflow(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "key", Type: ColumnTypeString, Width: 4},
		},
	}

	rec := Record{
		Columns: map[string][]byte{
			"key": []byte("toolongvalue"),
		},
	}

	f := &storage.InMemoryStorage{Data: []byte{}}
	_, err := WriteRecordToFile(f, rec, schema)
	if err == nil {
		t.Fatal("expected error for column overflow, got nil")
	}
}

func TestSchemaTotalWidthExceedsRecord(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "a", Type: ColumnTypeString, Width: 100},
			{Name: "b", Type: ColumnTypeString, Width: 100},
		},
	}

	rec := Record{
		Columns: map[string][]byte{
			"a": []byte("x"),
			"b": []byte("y"),
		},
	}

	f := &storage.InMemoryStorage{Data: []byte{}}
	_, err := WriteRecordToFile(f, rec, schema)
	if err == nil {
		t.Fatal("expected error for schema exceeding record size, got nil")
	}
}
