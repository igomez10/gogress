package db

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

func Test_loadOperationsFromLogFile(t *testing.T) {
	type args struct {
		idx    map[string]int64
		reader io.Reader
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
				idx:    make(map[string]int64),
				reader: strings.NewReader("default:key1:1\ndefault:key2:2\n"),
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
				idx:    make(map[string]int64),
				reader: strings.NewReader("invalid\nlog\n"),
			},
			expectedErr: InvalidLogLineError,
		},
		{
			name: "empty log",
			args: args{
				idx:    make(map[string]int64),
				reader: strings.NewReader(""),
			},
			want: map[string]map[string]int64{},
		},
		{
			name: "overwrite old value",
			args: args{
				idx:    make(map[string]int64),
				reader: strings.NewReader("default:key1:1\ndefault:key1:2\n"),
			},
			want: map[string]map[string]int64{
				"default": {
					"key1": 2,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offsets, err := BuildIndex(tt.args.reader, map[string]ReadWriteCloserSeeker{})
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
