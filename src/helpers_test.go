package main

import (
	"fmt"
	"io"
	. "launchpad.net/gocheck"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

const ReplicaCount = 4

var masterCmd *exec.Cmd

func startMaster(t *C) {
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

func startReplicas(t *C) {
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

func startReplica(t *C, n int) {
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

func killMaster(t *C) {
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

func verify(t *C, check func() bool, successMessage string, failMessage string) {
	for i := 0; i < 500; i++ {
		if check() {
			log.Println(successMessage)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(failMessage)
}

func killAll(c *C) {
	killMaster(c)
	for _, replicaCmd := range replicas {
		replicaCmd.Process.Kill()
	}
}

func startCmd(t *C, path string, args ...string) *exec.Cmd {
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
