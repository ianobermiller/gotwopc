package main

import (
	"io/ioutil"
	"log"
	"os"
	"path"
)

type keyValueStore struct {
	basePath string
}

func newKeyValueStore(dbPath string) (store *keyValueStore) {
	err := os.MkdirAll(dbPath, 0)
	if err != nil {
		log.Fatalln("newKeyValueStore:", err)
	}
	store = &keyValueStore{dbPath}
	return
}

func (s *keyValueStore) getPath(key string) string {
	return path.Join(s.basePath, key)
}

func (s *keyValueStore) put(key string, value string) (err error) {
	err = ioutil.WriteFile(s.getPath(key), []byte(value), 0777)
	return
}

func (s *keyValueStore) del(key string) (err error) {
	err = os.Remove(s.getPath(key))
	return
}

func (s *keyValueStore) get(key string) (value string, err error) {
	bytes, err := ioutil.ReadFile(s.getPath(key))
	if err != nil {
		return
	}
	value = string(bytes)
	return
}
