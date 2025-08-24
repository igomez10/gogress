package db

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
)

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
					"default": func() io.ReadWriteSeeker {
						f, err := os.CreateTemp("", "")
						if err != nil {
							return nil
						}
						return f
					}(),
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
			name: "invalid log",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("invalid\nlog\n"),
				storage: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f, err := os.CreateTemp("", "")
						if err != nil {
							return nil
						}
						return f
					}(),
				},
			},
			expectedErr: InvalidLogLineError,
		},
		{
			name: "empty log",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader(""),
				storage: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f, err := os.CreateTemp("", "")
						if err != nil {
							return nil
						}
						return f
					}(),
				},
			},
			want: map[string]map[string]int64{},
		},
		{
			name: "overwrite old value",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("default:key1:1\ndefault:key1:2\n"),
				storage: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f, err := os.CreateTemp("", "")
						if err != nil {
							return nil
						}
						f.Write([]byte("key1:value1"))
						f.Write(bytes.Repeat([]byte{0}, paddingSize-len("key1:value1")))
						return f
					}(),
				},
			},
			want: map[string]map[string]int64{
				"default": {
					"key1": paddingSize * 1,
				},
			},
		},
		{
			name: "wal and storage are same",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("default:key1:value1\n"),
				storage: func() map[string]io.ReadWriteSeeker {
					res := map[string]io.ReadWriteSeeker{}
					f, err := os.CreateTemp("", "")
					if err != nil {
						return nil
					}
					res["default"] = f

					b := bytes.NewBuffer(nil)
					doc1 := "key1:value1"
					if _, err := b.WriteString(doc1); err != nil {
						return nil
					}
					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := b.Write(padding); err != nil {
						return nil
					}

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
			offsets, err := BuildIndex(tt.args.walIndex, tt.args.storage)
			if (err != nil && !errors.Is(err, tt.expectedErr)) || (err == nil && tt.expectedErr != nil) {
				t.Errorf("loadOperationsFromLogFile() error = %v, want %v", err, tt.expectedErr)
			}

			for tableName, idx := range tt.want {
				for k, v := range idx {
					if got := offsets[tableName][k]; got != v {
						t.Errorf("loadOperationsFromLogFile() = %v, want %v", got, v)
					}
				}
			}

		})
	}
}

func TestDB_CreateTable(t *testing.T) {
	type fields struct {
		LogFile io.ReadWriter
		Mutex   sync.RWMutex
		Tables  map[string]*Table
	}
	type args struct {
		tableName string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		expectedErr error
	}{
		{
			name: "create new table",
			fields: fields{
				LogFile: nil,
				Mutex:   sync.RWMutex{},
				Tables:  make(map[string]*Table),
			},
			args:        args{tableName: "new_table"},
			expectedErr: nil,
		},
		{
			name: "create existing table",
			fields: fields{
				LogFile: nil,
				Mutex:   sync.RWMutex{},
				Tables:  map[string]*Table{"existing_table": {}},
			},
			args:        args{tableName: "existing_table"},
			expectedErr: CreateTableAlreadyExistsError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				LogFile: tt.fields.LogFile,
				Mutex:   tt.fields.Mutex,
				Tables:  tt.fields.Tables,
			}
			if err := db.CreateTable(tt.args.tableName, &CreateTableOptions{}); err != tt.expectedErr {
				t.Errorf("DB.CreateTable() error = %v, expectedErr %v", err, tt.expectedErr)
			}
		})
	}
}

func TestBuildIndexFromStorage(t *testing.T) {
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
				storage: func() io.ReadSeeker {
					b := strings.Builder{}
					doc1 := "key1:value1"
					if _, err := b.WriteString(doc1); err != nil {
						return nil
					}
					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := b.Write(padding); err != nil {
						return nil
					}

					doc2 := "somelongkey:value2"
					if _, err := b.WriteString(doc2); err != nil {
						return nil
					}
					padding = bytes.Repeat([]byte{0}, paddingSize-len(doc2))
					if _, err := b.Write(padding); err != nil {
						return nil
					}

					doc3 := "short:k3"
					if _, err := b.WriteString(doc3); err != nil {
						return nil
					}
					padding = bytes.Repeat([]byte{0}, paddingSize-len(doc3))
					if _, err := b.Write(padding); err != nil {
						return nil
					}

					return strings.NewReader(b.String())
				}(),
			},
			want: map[string]int64{
				"key1":        0,
				"somelongkey": paddingSize * 1,
				"short":       paddingSize * 2,
			},
		},
		{
			name: "invalid line",
			args: args{
				storage: strings.NewReader("invalid\nlog\n"),
			},
			wantErr: true,
		},
		{
			name: "invalid line missing colon",
			args: args{
				storage: func() io.ReadSeeker {
					b := strings.Builder{}
					doc1 := "notcontainscolon"
					if _, err := b.WriteString(doc1); err != nil {
						return nil
					}
					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := b.Write(padding); err != nil {
						return nil
					}

					return strings.NewReader(b.String())
				}(),
			},
			wantErr: true,
		},
		{
			name: "last entry is invalid should return empty",
			args: args{
				storage: func() io.ReadSeeker {
					b := strings.Builder{}
					doc1 := "key:somevalue"
					if _, err := b.WriteString(doc1); err != nil {
						return nil
					}
					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := b.Write(padding); err != nil {
						return nil
					}

					doc2 := "invalidentry"
					if _, err := b.WriteString(doc2); err != nil {
						return nil
					}
					padding = bytes.Repeat([]byte{0}, paddingSize-len(doc2))
					if _, err := b.Write(padding); err != nil {
						return nil
					}

					return strings.NewReader(b.String())
				}(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildIndexFromStorage(tt.args.storage)
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
	type args struct {
		finalIndex   map[string]map[string]int64
		changeLog    map[string]map[string]string
		storageFiles map[string]io.ReadWriteSeeker
	}
	tests := []struct {
		name                 string
		args                 args
		expectedTotalChanges int64
		expectedFinalIndex   map[string]map[string]int64
		expectedStorageFiles map[string]io.ReadWriteSeeker
		wantErr              bool
	}{
		{
			name: "nothing to reconcile",
			args: args{
				finalIndex: map[string]map[string]int64{
					"default": {
						"key1": 0,
					},
				},
				changeLog: map[string]map[string]string{
					"default": {
						"key1": "value1",
					},
				},
				storageFiles: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f, err := os.CreateTemp("", "")
						if err != nil {
							return nil
						}
						doc1 := "key1:value1"
						if _, err := f.WriteString(doc1); err != nil {
							return nil
						}
						padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
						if _, err := f.Write(padding); err != nil {
							return nil
						}

						return f
					}(),
				},
			},
			expectedFinalIndex: map[string]map[string]int64{
				"default": {
					"key1": 0,
				},
			},
			expectedStorageFiles: map[string]io.ReadWriteSeeker{
				"default": func() io.ReadWriteSeeker {
					f, err := os.CreateTemp("", "")
					if err != nil {
						return nil
					}
					doc1 := "key1:value1"
					if _, err := f.WriteString(doc1); err != nil {
						return nil
					}
					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := f.Write(padding); err != nil {
						return nil
					}

					return f
				}(),
			},
			expectedTotalChanges: 0,
		},
		{
			name: "reconcile 1 change, same key different value",
			args: args{
				finalIndex: map[string]map[string]int64{
					"default": {
						"key1": 0,
					},
				},
				changeLog: map[string]map[string]string{
					"default": {
						"key1": "newvalue1",
					},
				},
				storageFiles: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f, err := os.CreateTemp("", "")
						if err != nil {
							return nil
						}
						doc1 := "key1:value1"
						if _, err := f.WriteString(doc1); err != nil {
							return nil
						}
						padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
						if _, err := f.Write(padding); err != nil {
							return nil
						}

						return f
					}(),
				},
			},
			expectedFinalIndex: map[string]map[string]int64{
				"default": {
					"key1": paddingSize * 1,
				},
			},
			expectedStorageFiles: map[string]io.ReadWriteSeeker{
				"default": func() io.ReadWriteSeeker {
					f, err := os.CreateTemp("", "")
					if err != nil {
						return nil
					}
					doc1 := "key1:value1"
					if _, err := f.WriteString(doc1); err != nil {
						return nil
					}
					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := f.Write(padding); err != nil {
						return nil
					}

					doc2 := "key1:newvalue1"
					if _, err := f.WriteString(doc2); err != nil {
						return nil
					}
					padding = bytes.Repeat([]byte{0}, paddingSize-len(doc2))
					if _, err := f.Write(padding); err != nil {
						return nil
					}

					return f
				}(),
			},
			expectedTotalChanges: 1,
		},
		{
			name: "reconcile 1 change, new key",
			args: args{
				finalIndex: map[string]map[string]int64{
					"default": {
						"key1": 0,
					},
				},
				changeLog: map[string]map[string]string{
					"default": {
						"key1": "newvalue1",
						"key2": "newvalue2",
					},
				},
				storageFiles: map[string]io.ReadWriteSeeker{
					"default": func() io.ReadWriteSeeker {
						f, err := os.CreateTemp("", "")
						if err != nil {
							return nil
						}
						doc1 := "key1:value1"
						if _, err := f.WriteString(doc1); err != nil {
							return nil
						}
						padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
						if _, err := f.Write(padding); err != nil {
							return nil
						}

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
			expectedStorageFiles: map[string]io.ReadWriteSeeker{
				"default": func() io.ReadWriteSeeker {
					f, err := os.CreateTemp("", "")
					if err != nil {
						return nil
					}
					doc1 := "key1:value1"
					if _, err := f.WriteString(doc1); err != nil {
						return nil
					}
					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := f.Write(padding); err != nil {
						return nil
					}

					doc2 := "key1:newvalue1"
					if _, err := f.WriteString(doc2); err != nil {
						return nil
					}
					padding = bytes.Repeat([]byte{0}, paddingSize-len(doc2))
					if _, err := f.Write(padding); err != nil {
						return nil
					}

					doc3 := "key2:newvalue2"
					if _, err := f.WriteString(doc3); err != nil {
						return nil
					}
					padding = bytes.Repeat([]byte{0}, paddingSize-len(doc3))
					if _, err := f.Write(padding); err != nil {
						return nil
					}

					return f
				}(),
			},
			expectedTotalChanges: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			totalChanges, err := ReconcileChanges(tt.args.finalIndex, tt.args.changeLog, tt.args.storageFiles)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ReconcileChanges() error = %v, wantErr %v", err, tt.wantErr)
			}
			if totalChanges != tt.expectedTotalChanges {
				t.Fatalf("ReconcileChanges() = %v, want %v", totalChanges, tt.expectedTotalChanges)
			}

			if !reflect.DeepEqual(tt.args.finalIndex, tt.expectedFinalIndex) {
				t.Fatalf("ReconcileChanges() = %v, want %v", tt.args.finalIndex, tt.expectedFinalIndex)
			}

			// compare storage files
			if len(tt.args.storageFiles) != len(tt.expectedStorageFiles) {
				t.Errorf("ReconcileChanges() = %v, want %v", tt.args.storageFiles, tt.expectedStorageFiles)
			}

			for i := range tt.args.storageFiles {
				got := tt.args.storageFiles[i]
				expected := tt.expectedStorageFiles[i]

				gotBytes, err := io.ReadAll(got)
				if err != nil {
					t.Errorf("ReconcileChanges() error reading got = %v", err)
				}
				expectedBytes, err := io.ReadAll(expected)
				if err != nil {
					t.Errorf("ReconcileChanges() error reading expected = %v", err)
				}

				if string(gotBytes) != string(expectedBytes) {
					t.Errorf("ReconcileChanges() = %v, want %v", string(gotBytes), string(expectedBytes))
				}
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
	// Do not defer f.Close/remove here; DeleteTable should handle it
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

func TestDB_DeleteTable_TableNotFound(t *testing.T) {
	db := &DB{Tables: map[string]*Table{"default": {}}}
	if err := db.DeleteTable("missing"); err == nil {
		t.Fatalf("DeleteTable(missing) expected error, got nil")
	}
}
