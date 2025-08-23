package db

import (
	"errors"
	"io"
	"strings"
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
		want        map[string]int64
		expectedErr error
	}{
		{
			name: "valid log",
			args: args{
				idx:    make(map[string]int64),
				reader: strings.NewReader("key1:1\nkey2:2\n"),
			},
			want: map[string]int64{
				"key1": 1,
				"key2": 2,
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
			want: map[string]int64{},
		},
		{
			name: "overwrite old value",
			args: args{
				idx:    make(map[string]int64),
				reader: strings.NewReader("key1:1\nkey1:2\n"),
			},
			want: map[string]int64{
				"key1": 2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offsets, err := LoadOffsetsFromFile(tt.args.reader)
			if (err != nil && !errors.Is(err, tt.expectedErr)) || (err == nil && tt.expectedErr != nil) {
				t.Errorf("loadOperationsFromLogFile() error = %v, want %v", err, tt.expectedErr)
			}

			for k, v := range tt.want {
				if got := offsets[k]; got != v {
					t.Errorf("loadOperationsFromLogFile() = %v, want %v", got, v)
				}
			}

		})
	}
}
