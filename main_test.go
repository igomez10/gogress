package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/igomez10/gogress/pkg/db"
)

func TestBasicPutGet(t *testing.T) {
	// try to remove /tmp/db-
	db, err := db.NewDB(db.NewDBOptions{})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("key1")
	val := []byte("value1")
	if err := db.Tables["default"].Put(key, val, func(i int64) {}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := db.Tables["default"].Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected value to be found")
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("expected %q, got %q", val, got)
	}
}

func TestPut(t *testing.T) {
	db, err := db.NewDB(db.NewDBOptions{})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("key1")
	val := []byte("value1")
	if err := db.Tables["default"].Put(key, val, func(i int64) {}); err != nil {
		t.Fatal(err)
	}

	f := db.Tables["default"].Storage
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	// check that value was written
	content, err := io.ReadAll(db.Tables["default"].Storage)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, val) {
		t.Fatalf("expected %q to be in %q", val, content)
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
	// Verify that the record was written correctly
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
	db, err := db.NewDB(db.NewDBOptions{})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("key1")
	val1 := []byte("value1")
	if err := db.Tables["default"].Put(key, val1, func(i int64) {}); err != nil {
		t.Fatal(err)
	}

	val2 := []byte("value2")
	if err := db.Tables["default"].Put(key, val2, func(i int64) {}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := db.Tables["default"].Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected value to be found")
	}
	if !bytes.Equal(got, val2) {
		t.Fatalf("expected %q, got %q", val2, got)
	}
	// expect to contain first value too in underlying file
	if _, err := db.Tables["default"].Storage.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(db.Tables["default"].Storage)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, val1) {
		t.Fatalf("expected %q to be in %q", val1, content)
	}
}
