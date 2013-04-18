package gotwopc

import (
	"fmt"
	. "github.com/robertkrimen/terst"
	"os"
	"testing"
)

var store *keyValueStore

func Test(t *testing.T) {
	os.Remove("./test.db")
	s, err := newKeyValueStore("./test.db")
	if err != nil {
		t.Fatal("Failed to newKeyValueStore:", err)
	}
	store = s
}

func TestAll(t *testing.T) {
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

func TestMultiple(t *testing.T) {
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
