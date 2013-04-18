package gotwopc

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

type keyValueStore struct {
	db      *sql.DB
	putStmt *sql.Stmt
	delStmt *sql.Stmt
	getStmt *sql.Stmt
}

func newKeyValueStore(dbPath string) (store *keyValueStore, err error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = db.Exec("create table if not exists data (key text not null primary key, value text)")
	if err != nil {
		fmt.Printf("Couldn't create table: %q\n", err)
		return
	}

	putStmt, err := db.Prepare("insert or replace into data(key, value) values(?, ?)")
	if err != nil {
		fmt.Printf("Couldn't prepare put statement: %q\n", err)
		return
	}

	delStmt, err := db.Prepare("delete from data where key = ?")
	if err != nil {
		fmt.Printf("Couldn't prepare put statement: %q\n", err)
		return
	}

	getStmt, err := db.Prepare("select value from data where key = ?")
	if err != nil {
		fmt.Printf("Couldn't prepare put statement: %q\n", err)
		return
	}

	store = &keyValueStore{db, putStmt, delStmt, getStmt}
	return
}

func (s *keyValueStore) put(key string, value string) (err error) {
	_, err = s.putStmt.Exec(key, value)
	return
}

func (s *keyValueStore) del(key string) (err error) {
	_, err = s.delStmt.Exec(key)
	return
}

func (s *keyValueStore) get(key string) (value string, err error) {
	row := s.getStmt.QueryRow(key)
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
