package main

import (
	"fmt"
	. "github.com/robertkrimen/terst"
	"os"
	"testing"
)

var store *keyValueStore

func TestKeyValueStoreSetup(t *testing.T) {
	os.Remove("./test.db")
	store = newKeyValueStore("./test.db")
}

func TestGetWithoutPut(t *testing.T) {
	Terst(t)

	_, err := store.get("nonexistentvalue")
	if err == nil {
		t.Error("Entry foo should not exist.")
	}
}

func TestKeyValueStoreAll(t *testing.T) {
	Terst(t)
	err := store.put("foo", "bar")
	if err != nil {
		t.Fatal("Failed to put:", err)
	}

	val, err := store.get("foo")
	if err != nil {
		t.Fatal("Failed to get:", err)
	}

	Is(val, "bar")

	err = store.del("foo")
	if err != nil {
		t.Fatal("Failed to del:", err)
	}

	val, err = store.get("foo")
	if err == nil {
		t.Error("Entry foo should not exist.")
	}
}

func TestKeyValueStoreMultiple(t *testing.T) {
	Terst(t)
	const count = 10

	for i := 0; i < count; i += 1 {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		err := store.put(key, val)
		if err != nil {
			t.Fatal("Failed to put:", err)
		}
	}

	for i := 0; i < count; i += 1 {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		v, err := store.get(key)
		if err != nil {
			t.Fatal("Failed to get:", err)
		}

		Is(v, val)
	}
}

func BenchmarkKeyValueStorePut(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		err := store.put(key, val)
		if err != nil {
			b.Fatal("Failed to put:", err)
		}
	}
}
