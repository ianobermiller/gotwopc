// +build !goci
package main

import (
	. "launchpad.net/gocheck"
	"log"
	"os"
	"sync"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func TestMain(t *testing.T) { TestingT(t) }

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

func (s *MainSuite) SetUpTest(c *C) {
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

	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	const clients = 4
	var wg sync.WaitGroup
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			client := NewMasterClient(MasterPort)
			failedCount := 0
			for j := 0; j < 5; j++ {
				err := client.Put(keys[j], "bar")
				if err != nil {
					failedCount++
				}
			}
			c.Log("failedCount:", failedCount)
		}()
	}
	wg.Wait()
}
