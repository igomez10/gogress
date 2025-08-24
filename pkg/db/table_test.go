package db

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"
)

func TestReadRecordFromFile(t *testing.T) {
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
				f: func() io.ReadSeeker {
					b := bytes.NewBuffer(make([]byte, 0, paddingSize))
					doc1 := []byte("key1:value1")
					if _, err := b.Write(doc1); err != nil {
						return nil
					}

					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := b.Write(padding); err != nil {
						return nil
					}
					return bytes.NewReader(b.Bytes())
				}(),
				offset: 0,
			},
			want: Record{
				Key:   []byte("key1"),
				Value: []byte("value1"),
			},
			wantErr: false,
		},
		{
			name: "invalid record",
			args: args{
				f: func() io.ReadSeeker {
					b := bytes.NewBuffer(make([]byte, 0, paddingSize))
					doc1 := []byte("invalid")
					if _, err := b.Write(doc1); err != nil {
						return nil
					}

					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := b.Write(padding); err != nil {
						return nil
					}
					return bytes.NewReader(b.Bytes())
				}(),
				offset: 0,
			},
			want:    Record{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadRecordFromFile(tt.args.f, tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadRecordFromFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadRecordFromFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteRecordToFile(t *testing.T) {
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
				newRecord: Record{
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
				initialFile: func() io.ReadWriteSeeker {
					f, err := os.Create("/tmp/testfile")
					if err != nil {
						return nil
					}
					return f
				}(),
			},
			expectedFile: "key1:value1" + string(make([]byte, paddingSize-len("key1:value1"))),
			wantErr:      false,
		},
		{
			name: "valid record in non-empty file",
			args: args{
				newRecord: Record{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
				initialFile: func() io.ReadWriteSeeker {
					f, err := os.CreateTemp("", "")
					if err != nil {
						return nil
					}

					doc1 := []byte("key1:value1")
					if _, err := f.Write(doc1); err != nil {
						return nil
					}

					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := f.Write(padding); err != nil {
						return nil
					}
					return f
				}(),
			},
			expectedFile: "key1:value1" + string(make([]byte, paddingSize-len("key1:value1"))) +
				"key2:value2" + string(make([]byte, paddingSize-len("key2:value2"))),
			wantErr: false,
		},
		{
			name: "valid record in non-empty file with same existing record",
			args: args{
				newRecord: Record{
					Key:   []byte("key1"),
					Value: []byte("updatedvalue"),
				},
				initialFile: func() io.ReadWriteSeeker {
					f, _ := os.CreateTemp("", "")
					doc1 := []byte("key1:value1")
					if _, err := f.Write(doc1); err != nil {
						return nil
					}

					padding := bytes.Repeat([]byte{0}, paddingSize-len(doc1))
					if _, err := f.Write(padding); err != nil {
						return nil
					}
					return f
				}(),
			},
			expectedFile: "key1:value1" + string(make([]byte, paddingSize-len("key1:value1"))) +
				"key1:updatedvalue" + string(make([]byte, paddingSize-len("key1:updatedvalue"))),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := WriteRecordToFile(tt.args.initialFile, tt.args.newRecord); (err != nil) != tt.wantErr {
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
				t.Errorf("WriteRecordToFile() = %v, want %v", gotF, tt.expectedFile)
			}
		})
	}
}
