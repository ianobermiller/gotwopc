package main

import (
	"github.com/peterbourgon/diskv"
)

type keyValueStore struct {
	d *diskv.Diskv
}

func flatTransform(s string) []string { return []string{} }

func newKeyValueStore(dbPath string) (store *keyValueStore, err error) {
	d := diskv.New(diskv.Options{
		BasePath:     dbPath,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024,
	})

	store = &keyValueStore{d}
	return
}

func (s *keyValueStore) put(key string, value string) (err error) {
	err = s.d.Write(key, []byte(value))
	return
}

func (s *keyValueStore) del(key string) (err error) {
	err = s.d.Erase(key)
	return
}

func (s *keyValueStore) get(key string) (value string, err error) {
	bytes, err := s.d.Read(key)
	value = string(bytes)
	return
}
