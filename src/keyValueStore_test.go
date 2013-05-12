// +build !goci
package main

import (
	"fmt"
	. "launchpad.net/gocheck"
	"os"
)

var testDbPath = "./test.db"

type KeyValueStoreSuite struct{}

var _ = Suite(&KeyValueStoreSuite{})

func (s *KeyValueStoreSuite) TearDownTest(c *C) {
	os.RemoveAll("./test.db")
}

func (s *KeyValueStoreSuite) TestGetWithoutPut(c *C) {
	store := newKeyValueStore(testDbPath)
	_, err := store.get("nonexistentvalue")
	if err == nil {
		c.Error("Entry foo should not exist.")
	}
}

func (s *KeyValueStoreSuite) TestDeleteNonexistent(c *C) {
	store := newKeyValueStore(testDbPath)
	err := store.del("nonexistentvalue")
	if err != nil {
		c.Error("KVS should not error when deleting nonexistent value: ", err)
	}
}

func (s *KeyValueStoreSuite) TestKeyValueStoreAll(c *C) {
	store := newKeyValueStore(testDbPath)
	err := store.put("foo", "bar")
	if err != nil {
		c.Fatal("Failed to put:", err)
	}

	val, err := store.get("foo")
	if err != nil {
		c.Fatal("Failed to get:", err)
	}

	c.Assert(val, Equals, "bar")

	err = store.del("foo")
	if err != nil {
		c.Fatal("Failed to del:", err)
	}

	val, err = store.get("foo")
	if err == nil {
		c.Error("Entry foo should not exist.")
	}
}

func (s *KeyValueStoreSuite) TestKeyValueStoreMultiple(c *C) {
	store := newKeyValueStore(testDbPath)
	const count = 10

	for i := 0; i < count; i += 1 {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		err := store.put(key, val)
		if err != nil {
			c.Fatal("Failed to put:", err)
		}
	}

	for i := 0; i < count; i += 1 {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		v, err := store.get(key)
		if err != nil {
			c.Fatal("Failed to get:", err)
		}

		c.Assert(v, Equals, val)
	}
}

func (s *KeyValueStoreSuite) TestList(c *C) {
	store := newKeyValueStore(testDbPath)
	const count = 10

	for i := 0; i < count; i += 1 {
		store.put(fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i))
	}

	keys, err := store.list()
	if err != nil {
		c.Fatal("Failed to list:", err)
	}

	for i, key := range keys {
		if key != fmt.Sprintf("key%v", i) {
			c.Fatal("Key #", i, "was", key, "expected", fmt.Sprintf("key%v", i))
		}
	}
}

func (s *KeyValueStoreSuite) BenchmarkKeyValueStorePut(c *C) {
	store := newKeyValueStore(testDbPath)
	for i := 0; i < c.N; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		err := store.put(key, val)
		if err != nil {
			c.Fatal("Failed to put:", err)
		}
	}
}
