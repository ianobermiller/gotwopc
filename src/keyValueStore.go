package gotwopc

import (
	"code.google.com/p/leveldb-go/leveldb"
	leveldbdb "code.google.com/p/leveldb-go/leveldb/db"
	"fmt"
)

type keyValueStore struct {
	db *leveldb.DB
}

func newKeyValueStore(dbPath string) (store *keyValueStore, err error) {
	db, err := leveldb.Open("./testdb", &leveldbdb.Options{})
	if err != nil {
		fmt.Printf("Couldn't create db: %q\n", err)
		return
	}

	store = &keyValueStore{db}
	return
}

func (s *keyValueStore) put(key string, value string) (err error) {
	err = s.db.Set([]byte(key), []byte(value), nil)
	return
}

func (s *keyValueStore) del(key string) (err error) {
	err = s.db.Delete([]byte(key), nil)
	return
}

func (s *keyValueStore) get(key string) (value string, err error) {
	bytes, err := s.db.Get([]byte(key), nil)
	value = string(bytes)
	return
}

func (s *keyValueStore) close() {
	s.db.Close()
}
