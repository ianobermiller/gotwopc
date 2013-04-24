package main

import (
	. "github.com/robertkrimen/terst"
	"os"
	"os/exec"
	"testing"
)

var _ = os.DevNull

func TestStartAndKillMaster(t *testing.T) {
	Terst(t)

	startMaster(t)

	client := NewMasterClient(MasterPort)

	_, err := client.Get("foo")
	if err != nil {
		t.Fatal("Unable to get foo:", err)
	}

	killMaster(t)

	_, err = client.Get("foo")

	IsNot(err, nil)
}

var masterCmd *exec.Cmd

func startMaster(t *testing.T) {
	masterCmd = exec.Command("src.exe", "-m")
	err := masterCmd.Start()
	if err != nil {
		t.Fatal("Unable to Run src.exe")
	}

	client := NewMasterClient(MasterPort)

	for i := 0; i < 10; i++ {
		_, err = client.Get("whatever")
		if err == nil {
			return
		}
	}
	t.Fatal("Unable to Get after running Master:", err)
}

func killMaster(t *testing.T) {
	_ = masterCmd.Process.Kill()

	client := NewMasterClient(MasterPort)

	for i := 0; i < 10; i++ {
		_, err := client.Get("whatever")
		if err != nil {
			return
		}
	}
	t.Fatal("Able to Get after running Master")
}
