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
}

func (s *MainSuite) SetUpTest(c *C) {
	// Clean out data from old runs
	err := os.RemoveAll("data")
	if err != nil {
		c.Fatal("RemoveAll(data) failed: ", err)
	}
	err = os.RemoveAll("logs")
	if err != nil {
		c.Fatal("RemoveAll(logs) failed: ", err)
	}
}

func (s *MainSuite) TearDownTest(c *C) {
	killAll(c)
}

func (s *MainSuite) TestStartAndKillMaster(c *C) {
	startMaster(c)

	client := NewMasterClient(MasterPort)

	killMaster(c)

	_, err := client.Ping("foo")

	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestPutGetAndDelFromReplicas(c *C) {
	startReplicas(c, false)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.PutTest("foo", "bar", MasterDontDie, make([]ReplicaDeath, 4))
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
			c.Assert(err, Equals, nil)
			c.Assert(*val, Equals, "bar")
		}(i)
	}
	wg.Wait()

	err = client.DelTest("foo", MasterDontDie, make([]ReplicaDeath, 4))
	c.Assert(err, Equals, nil)

	// no replica should have the value
	wg.Add(ReplicaCount)
	for i := 0; i < ReplicaCount; i++ {
		go func(i int) {
			_, err := client.GetTest("foo", i)
			if err == nil {
				c.Error("Del failed.")
			}
			wg.Done()
			c.Assert(err, Not(Equals), nil)
		}(i)
	}
	wg.Wait()
}

func (s *MainSuite) TestReplicaShouldAbortIfPutOnLockedKey(c *C) {
	startReplicas(c, false)
	startMaster(c)
	client := NewReplicaClient(GetReplicaHost(0))

	ok, err := client.TryPut("foo", "bar1", "tx1", ReplicaDontDie)
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, true)

	ok, err = client.TryPut("foo", "bar2", "tx2", ReplicaDontDie)
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, false)

	ok, err = client.Commit("tx1", ReplicaDontDie)
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, true)
}

func (s *MainSuite) TestReplicaShouldAbortIfDelOnLockedKey(c *C) {
	startReplicas(c, false)
	startMaster(c)

	client := NewReplicaClient(GetReplicaHost(0))

	ok, err := client.TryPut("foo", "bar1", "tx1", ReplicaDontDie)
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, true)

	ok, err = client.TryDel("foo", "tx2", ReplicaDontDie)
	c.Assert(err, Equals, nil)
	c.Assert(*ok, Equals, false)
}

func (s *MainSuite) TestReplicaShouldErrOnUnknownTxCommit(c *C) {
	startReplicas(c, false)
	startMaster(c)

	client := NewReplicaClient(GetReplicaHost(0))

	_, err := client.Commit("tx1", ReplicaDontDie)
	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestReplicaShouldErrOnUnknownTxAbort(c *C) {
	startReplicas(c, false)
	startMaster(c)

	client := NewReplicaClient(GetReplicaHost(0))

	_, err := client.Abort("tx1")
	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestReplicaShouldErrWithStress(c *C) {
	startReplicas(c, false)
	startMaster(c)

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
				err := client.PutTest(keys[j], "bar", MasterDontDie, make([]ReplicaDeath, ReplicaCount))
				if err != nil {
					failedCount++
				}
			}
			c.Log("failedCount:", failedCount)
		}()
	}
	wg.Wait()
}

func (s *MainSuite) TestTxShouldAbortIfReplicaDiesAtStartOfPut(c *C) {
	startReplicas(c, false)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.PutTest("foo", "bar", MasterDontDie, []ReplicaDeath{ReplicaDieBeforeProcessingMutateRequest, ReplicaDontDie, ReplicaDontDie, ReplicaDontDie})
	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestTxShouldAbortIfReplicaDiesAfterLoggingButBeforeSendingPrepared(c *C) {
	startReplicas(c, false)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.PutTest("foo", "bar", MasterDontDie, []ReplicaDeath{ReplicaDontDie, ReplicaDontDie, ReplicaDontDie, ReplicaDieAfterLoggingPrepared})
	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestTxShouldCommitIfReplicaDiesBeforeProcessingCommit(c *C) {
	startReplicas(c, true)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.PutTest("foo", "bar", MasterDontDie, []ReplicaDeath{ReplicaDontDie, ReplicaDieBeforeProcessingCommit, ReplicaDontDie, ReplicaDontDie})
	c.Assert(err, Equals, nil)
}

func (s *MainSuite) TestTxShouldCommitIfReplicaDiesAfterLoggingCommit(c *C) {
	startReplicas(c, true)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.PutTest("foo", "bar", MasterDontDie, []ReplicaDeath{ReplicaDontDie, ReplicaDieAfterLoggingCommitted, ReplicaDontDie, ReplicaDontDie})
	c.Assert(err, Equals, nil)

	// Make sure the replica that died still committed
	val, err := client.GetTest("foo", 1)
	c.Assert(err, Equals, nil)
	c.Assert(*val, Equals, "bar")
}

func (s *MainSuite) TestTxShouldCommitIfReplicaDiesAfterDeletingDataFromTemp(c *C) {
	startReplicas(c, true)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.PutTest("foo", "bar", MasterDontDie, []ReplicaDeath{ReplicaDontDie, ReplicaDieAfterDeletingFromTempStore, ReplicaDontDie, ReplicaDontDie})
	c.Assert(err, Equals, nil)

	// Make sure the replica that died still committed
	val, err := client.GetTest("foo", 1)
	c.Assert(err, Equals, nil)
	c.Assert(*val, Equals, "bar")
}

func (s *MainSuite) TestPutGetDelFromMaster(c *C) {
	startReplicas(c, true)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.Put("TestPutGetDelFromMaster", "super")
	c.Assert(err, Equals, nil)

	val, err := client.Get("TestPutGetDelFromMaster")
	c.Assert(err, Equals, nil)
	c.Assert(*val, Equals, "super")

	err = client.Del("TestPutGetDelFromMaster")
	c.Assert(err, Equals, nil)

	val, err = client.Get("TestPutGetDelFromMaster")
	c.Assert(err, Not(Equals), nil)
}

func (s *MainSuite) TestTxShouldAbortIfMasterDiesBeforeLoggingCommitted(c *C) {
	startReplicas(c, true)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.PutTest("DiedBefore", "first", MasterDieBeforeLoggingCommitted, make([]ReplicaDeath, 4))
	c.Assert(err, Not(Equals), nil)

	startMaster(c)
	// Master should recover and issue abort to all replicas, so a subsequent put on the same key should succeed
	// (they shouldn't be locking the key)

	err = client.Put("DiedBefore", "second")
	c.Assert(err, Equals, nil)

	val, err := client.Get("DiedBefore")
	c.Assert(err, Equals, nil)
	c.Assert(*val, Equals, "second")
}

func (s *MainSuite) TestTxShouldCommitIfMasterDiesAfterLoggingCommitted(c *C) {
	startReplicas(c, true)
	startMaster(c)

	client := NewMasterClient(MasterPort)

	err := client.PutTest("DiedAfter", "shazam", MasterDieAfterLoggingCommitted, make([]ReplicaDeath, 4))
	c.Assert(err, Not(Equals), nil)

	startMaster(c)
	// Master should recover and issue commit to all replicas (bringing them out of the uncertain state),
	// so a subsequent get should return the correct value

	val, err := client.Get("DiedAfter")
	c.Assert(err, Equals, nil)
	c.Assert(*val, Equals, "shazam")
}
