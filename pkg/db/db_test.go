package db

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func Test_RebuildIndex(t *testing.T) {
	type args struct {
		idx      map[string]int64
		walIndex io.Reader
		storage  map[string]io.ReadSeeker
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
				storage:  map[string]io.ReadSeeker{"default": nil},
			},
			want: map[string]map[string]int64{
				"default": {
					"key1": 1,
					"key2": 2,
				},
			},
		},
		{
			name: "invalid log",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("invalid\nlog\n"),
				storage:  map[string]io.ReadSeeker{"default": nil},
			},
			expectedErr: InvalidLogLineError,
		},
		{
			name: "empty log",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader(""),
				storage:  map[string]io.ReadSeeker{"default": nil},
			},
			want: map[string]map[string]int64{},
		},
		{
			name: "overwrite old value",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("default:key1:1\ndefault:key1:2\n"),
				storage:  map[string]io.ReadSeeker{"default": nil},
			},
			want: map[string]map[string]int64{
				"default": {
					"key1": 2,
				},
			},
		},
		{
			name: "merge with existing index",
			args: args{
				idx:      make(map[string]int64),
				walIndex: strings.NewReader("default:key1:1\ndefault:key2:2\n"),
				storage: map[string]io.ReadSeeker{
					"users": strings.NewReader("users:key1:1\nusers:key2:2\n"),
				},
			},
			want: map[string]map[string]int64{
				"default": {
					"key1": 1,
					"key2": 2,
				},
				"users": {
					"key1": 1,
					"key2": 2,
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
