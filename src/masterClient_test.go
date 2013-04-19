package main

import (
	. "github.com/robertkrimen/terst"
	"testing"
)

func TestMasterClient(t *testing.T) {
	Terst(t)
	startMaster()

	client := NewClient()
	// err := client.Put("foo", "bar")
	// if err != nil {
	// 	t.Error("Unable to put foo")
	// }
	val, err := client.Get("foo")
	if err != nil {
		t.Error("Unable to get foo")
	}

	Is(val, "hello")
}
