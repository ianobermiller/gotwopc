package main

import (
	. "github.com/robertkrimen/terst"
	"os"
	"os/exec"
	"testing"
)

var _ = os.DevNull

var masterCmd *exec.Cmd

func startMaster(t *testing.T) {
	masterCmd = exec.Command("src.exe", "-m")
	err := masterCmd.Start()
	if err != nil {
		t.Fatal("Unable to Run src.exe")
	}

	client := NewClient()

	for {
		_, err := client.Get("whatever")
		if err == nil {
			break
		}
	}
}

func killMaster(t *testing.T) {
	_ = masterCmd.Process.Kill()

	client := NewClient()

	for {
		_, err := client.Get("whatever")
		if err != nil {
			break
		}
	}
}

func TestStartAndKillMaster(t *testing.T) {
	Terst(t)

	startMaster(t)

	client := NewClient()

	_, err := client.Get("foo")
	if err != nil {
		t.Fatal("Unable to get foo")
	}

	killMaster(t)

	_, err = client.Get("foo")

	IsNot(err, nil)
}
