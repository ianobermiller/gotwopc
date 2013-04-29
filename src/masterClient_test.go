package main

import (
	"fmt"
	. "github.com/robertkrimen/terst"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	. "testing"
	"time"
)

const ReplicaCount = 4

var _ = os.DevNull

func init() {
	log.SetPrefix("C  ")
	log.SetFlags(0)
}

func disabledTestStartAndKillMaster(t *T) {
	Terst(t)

	startMaster(t)

	client := NewMasterClient(MasterPort)

	killMaster(t)

	_, err := client.Ping("foo")

	IsNot(err, nil)
}

func TestPutAndGetFromReplicas(t *T) {
	Terst(t)

	startReplicas(t)
	startMaster(t)
	defer killAll(t)

	client := NewMasterClient(MasterPort)

	err := client.Put("foo", "bar")
	Is(err, nil)

	// every replica should have the value
	var wg sync.WaitGroup
	wg.Add(ReplicaCount)
	for i := 0; i < ReplicaCount; i++ {
		go func(i int) {
			val, err := client.GetTest("foo", i)
			if err != nil || *val != "bar" {
				t.Error("Get failed.")
			}
			wg.Done()
			//Is(err, nil)
			//Is(*val, "bar")
		}(i)
	}
	wg.Wait()
}

var masterCmd *exec.Cmd

func startMaster(t *T) {
	masterCmd = startCmd(t, "src.exe", "-m", "-n", strconv.Itoa(ReplicaCount))

	client := NewMasterClient(MasterPort)

	verify(t,
		func() bool {
			_, err := client.Ping("whatever")
			return err == nil
		},
		"Ping to Master successful.",
		"Unable to Ping after running Master.")
}

var replicas = [ReplicaCount]*exec.Cmd{}

func startReplicas(t *T) {
	var wg sync.WaitGroup
	for i := 0; i < ReplicaCount; i++ {
		wg.Add(1)
		go func(i int) {
			startReplica(t, i)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func startReplica(t *T, n int) {
	replicas[n] = startCmd(t, "src.exe", "-r", "-i", strconv.Itoa(n))

	client := NewReplicaClient(GetReplicaHost(n))

	verify(t,
		func() bool {
			_, err := client.Ping("whatever")
			return err == nil
		},
		fmt.Sprintf("Ping to Replica %v successful.", n),
		fmt.Sprintf("Unable to Ping after running Replica %v.", n))
}

func killMaster(t *T) {
	masterCmd.Process.Kill()

	client := NewMasterClient(MasterPort)

	verify(t,
		func() bool {
			_, err := client.Ping("whatever")
			return err != nil
		},
		"Master killed successfully.",
		"Able to Ping after running Master.")
}

func verify(t *T, check func() bool, successMessage string, failMessage string) {
	for i := 0; i < 500; i++ {
		if check() {
			log.Println(successMessage)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(failMessage)
}

func killAll(t *T) {
	killMaster(t)
	for _, replicaCmd := range replicas {
		replicaCmd.Process.Kill()
	}
}

func startCmd(t *T, path string, args ...string) *exec.Cmd {
	cmd := exec.Command(path, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Start()
	if err != nil {
		t.Fatal(err)
	}

	go io.Copy(os.Stdout, stdout)
	go io.Copy(os.Stderr, stderr)

	return cmd
}
