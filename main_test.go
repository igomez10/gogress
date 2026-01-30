package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/igomez10/gogress/pkg/db"
)

func TestBasicPutGet(t *testing.T) {
	d, err := db.NewDB(db.NewDBOptions{})
	if err != nil {
		t.Fatal(err)
	}

	colValues := map[string][]byte{
		"key":   []byte("key1"),
		"value": []byte("value1"),
	}
	if err := d.Tables["default"].Put(colValues, func() {}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := d.Tables["default"].Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected value to be found")
	}
	if !bytes.Equal(got.Columns["value"], []byte("value1")) {
		t.Fatalf("expected %q, got %q", "value1", got.Columns["value"])
	}
}

func TestPut(t *testing.T) {
	d, err := db.NewDB(db.NewDBOptions{})
	if err != nil {
		t.Fatal(err)
	}

	colValues := map[string][]byte{
		"key":   []byte("key1"),
		"value": []byte("value1"),
	}
	if err := d.Tables["default"].Put(colValues, func() {}); err != nil {
		t.Fatal(err)
	}

	f := d.Tables["default"].Storage
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	content, err := io.ReadAll(d.Tables["default"].Storage)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, []byte("value1")) {
		t.Fatalf("expected %q to be in %q", "value1", content)
	}
}

func TestWriteToFile(t *testing.T) {
	f, err := os.Create("/tmp/gogressdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	if _, err := f.Write([]byte("aloha")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, []byte("aloha")) {
		t.Fatalf("expected %q to be in %q", []byte("aloha"), content)
	}
}

func TestOverWriteExistingKey(t *testing.T) {
	d, err := db.NewDB(db.NewDBOptions{})
	if err != nil {
		t.Fatal(err)
	}

	colValues1 := map[string][]byte{
		"key":   []byte("key1"),
		"value": []byte("value1"),
	}
	if err := d.Tables["default"].Put(colValues1, func() {}); err != nil {
		t.Fatal(err)
	}

	colValues2 := map[string][]byte{
		"key":   []byte("key1"),
		"value": []byte("value2"),
	}
	if err := d.Tables["default"].Put(colValues2, func() {}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := d.Tables["default"].Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected value to be found")
	}
	if !bytes.Equal(got.Columns["value"], []byte("value2")) {
		t.Fatalf("expected %q, got %q", "value2", got.Columns["value"])
	}
	if _, err := d.Tables["default"].Storage.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(d.Tables["default"].Storage)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, []byte("value1")) {
		t.Fatalf("expected %q to be in %q", "value1", content)
	}
}
