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
	"testing"
	"time"
)

const ReplicaCount = 4

var _ = os.DevNull

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MainSuite struct{}

var _ = Suite(&MainSuite{})

func (s *MainSuite) SetUpSuite(c *C) {
	log.SetPrefix("C  ")
	log.SetFlags(0)
	log.SetOutput(NewConditionalWriter())

	// Clean out data from old runs
	os.RemoveAll("data")
	os.RemoveAll("logs")
}

func (s *MainSuite) TearDownTest(c *C) {
	// Clean out data from old runs
	os.RemoveAll("data")
	os.RemoveAll("logs")
}

func (s *MainSuite) TestStartAndKillMaster(c *C) {
	startMaster(c)

	client := NewMasterClient(MasterPort)

	killMaster(c)

	_, err := client.Ping("foo")

	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestPutAndGetFromReplicas(c *C) {
	startReplicas(c)
	startMaster(c)
	defer killAll(c)

	client := NewMasterClient(MasterPort)

	err := client.Put("foo", "bar")
	c.Assert(err, Equals, nil)

	// every replica should have the value
	var wg sync.WaitGroup
	wg.Add(ReplicaCount)
	for i := 0; i < ReplicaCount; i++ {
		go func(i int) {
			val, err := client.GetTest("foo", i)
			if err != nil || *val != "bar" {
				c.Error("Get failed.")
			}
			wg.Done()
			//c.Assert(err, Equals, nil)
			//Is(*val, "bar")
		}(i)
	}
	wg.Wait()
}

func (s *MainSuite) TestReplicaShouldAbortIfPutOnLockedKey(c *C) {
	startReplicas(c)
	startMaster(c)
	defer killAll(c)

	client := NewReplicaClient(GetReplicaHost(0))

	ok, err := client.TryPut("foo", "bar1", "tx1")
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, true)

	ok, err = client.TryPut("foo", "bar2", "tx2")
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, false)

	ok, err = client.Commit("tx1")
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, true)
}

func (s *MainSuite) TestReplicaShouldAbortIfDelOnLockedKey(c *C) {
	startReplicas(c)
	startMaster(c)
	defer killAll(c)

	client := NewReplicaClient(GetReplicaHost(0))

	ok, err := client.TryPut("foo", "bar1", "tx1")
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, true)

	ok, err = client.TryDel("foo", "tx2")
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, false)
}

func (s *MainSuite) TestReplicaShouldErrOnUnknownTxCommit(c *C) {
	startReplicas(c)
	startMaster(c)
	defer killAll(c)

	client := NewReplicaClient(GetReplicaHost(0))

	_, err := client.Commit("tx1")
	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestReplicaShouldErrOnUnknownTxAbort(c *C) {
	startReplicas(c)
	startMaster(c)
	defer killAll(c)

	client := NewReplicaClient(GetReplicaHost(0))

	_, err := client.Abort("tx1")
	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestReplicaShouldErrWithStress(c *C) {
	startReplicas(c)
	startMaster(c)
	defer killAll(c)

	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			client := NewMasterClient(MasterPort)
			failedCount := 0
			for j := 0; j < 5; j++ {
				err := client.Put("foo", "bar")
				if err != nil {
					failedCount++
				}
			}
			c.Log("failedCount:", failedCount)
		}()
	}
	wg.Wait()
}

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
