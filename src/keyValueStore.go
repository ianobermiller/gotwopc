package gotwopc

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

type keyValueStore struct {
	db *sql.DB
}

func newKeyValueStore(dbPath string) (*keyValueStore, error) {
	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	_, err = db.Exec("create table if not exists data (key text not null primary key, value text)")
	if err != nil {
		fmt.Printf("Couldn't create table: %q\n", err)
		return nil, err
	}

	return &keyValueStore{db}, nil
}

func (s *keyValueStore) put(key string, value string) (err error) {
	_, err = s.db.Exec("insert or replace into data(key, value) values(?, ?)", key, value)
	return
}

func (s *keyValueStore) del(key string) (err error) {
	_, err = s.db.Exec("delete from data where key = ?", key)
	return
}

func (s *keyValueStore) get(key string) (value string, err error) {
	row := s.db.QueryRow("select value from data where key = ?", key)
	err = row.Scan(&value)
	return
}

func (s *keyValueStore) close() {
	s.db.Close()
}

func queryPrepared(db *sql.DB, sql string, args ...interface{}) (result sql.Result, err error) {
	stmt, err := db.Prepare(sql)
	if err != nil {
		fmt.Printf("Couldn't prepare statement: %q\n", err)
		return
	}
	defer stmt.Close()

	result, err = stmt.Exec(args...)
	if err != nil {
		fmt.Printf("Couldn't execute statement: %q\n", err)
		return
	}
	return
}
