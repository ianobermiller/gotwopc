package main

import (
	"errors"
	"github.com/dchest/uniuri"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	TxAbortedError = errors.New("Transaction aborted.")
)

type Master struct {
	replicaCount int
	replicas     []*ReplicaClient
	log          *logger
	didSuicide   bool
}

type PutArgs struct {
	Key           string
	Value         string
	MasterDeath   MasterDeath
	ReplicaDeaths []ReplicaDeath
}

type GetArgs struct {
	Key string
}

type GetTestArgs struct {
	Key        string
	ReplicaNum int
}

type DelArgs struct {
	Key           string
	MasterDeath   MasterDeath
	ReplicaDeaths []ReplicaDeath
}

type PingArgs struct {
	Key string
}

type GetResult struct {
	Value string
}

func NewMaster(replicaCount int) *Master {
	l := newLogger("logs/master.txt")
	replicas := make([]*ReplicaClient, replicaCount)
	for i := 0; i < replicaCount; i++ {
		replicas[i] = NewReplicaClient(GetReplicaHost(i))
	}
	return &Master{replicaCount, replicas, l, false}
}

func (m *Master) Get(args *GetArgs, reply *GetResult) (err error) {
	return m.GetTest(&GetTestArgs{args.Key, -1}, reply)
}

func (m *Master) GetTest(args *GetTestArgs, reply *GetResult) (err error) {
	log.Println("Master.Get is being called")
	rn := args.ReplicaNum
	if rn < 0 {
		rn = rand.Intn(m.replicaCount)
	}
	r, err := m.replicas[rn].Get(args.Key)
	if err != nil {
		log.Printf("Master.Get: request to replica %v for key %v failed\n", rn, args.Key)
		return
	}
	reply.Value = *r
	return nil
}

func (m *Master) Del(args *DelArgs, _ *int) (err error) {
	return m.mutate(
		args.Key,
		args.ReplicaDeaths,
		func(r *ReplicaClient, txId string, i int) (*bool, error) {
			return r.TryDel(args.Key, txId, args.ReplicaDeaths[i])
		})
}

func (m *Master) Put(args *PutArgs, _ *int) (err error) {
	return m.mutate(
		args.Key,
		args.ReplicaDeaths,
		func(r *ReplicaClient, txId string, i int) (*bool, error) {
			return r.TryPut(args.Key, args.Value, txId, args.ReplicaDeaths[i])
		})
}

func (m *Master) mutate(key string, replicaDeaths []ReplicaDeath, f func(r *ReplicaClient, txId string, i int) (*bool, error)) (err error) {
	txId := uniuri.New()
	m.log.writeState(txId, Started)

	// Send out all TryPut requests in parallel
	// if any abort, send on the channel.
	// Channel must be buffered to allow the non-blocking read in the switch.
	shouldAbort := make(chan int, m.replicaCount)
	log.Println("Master.Put asking replicas to put tx:", txId, "key:", key)
	m.forEachReplica(func(i int, r *ReplicaClient) {
		success, err := f(r, txId, i)
		if err != nil {
			log.Println("Master.Put r.TryPut:", err)
		}
		if success == nil || !*success {
			shouldAbort <- 1
		}
	})

	// If at least one replica needed to abort
	select {
	case <-shouldAbort:
		log.Println("Master.Put asking replicas to abort tx:", txId, "key:", key)
		m.log.writeState(txId, Aborted)
		m.forEachReplica(func(i int, r *ReplicaClient) {
			_, err := r.Abort(txId, replicaDeaths[i])
			if err != nil {
				log.Println("Master.Put r.Abort:", err)
			}
		})
		return TxAbortedError
	default:
		break
	}

	// The transaction is now officially committed
	m.log.writeState(txId, Committed)

	log.Println("Master.Put asking replicas to commit tx:", txId, "key:", key)

	m.forEachReplica(func(i int, r *ReplicaClient) {
		for {
			_, err := r.Commit(txId, replicaDeaths[i])
			if err == nil {
				break
			}
			log.Println("Master.Put r.Commit:", err)
			time.Sleep(100 * time.Millisecond)
		}
	})

	return
}

func (m *Master) forEachReplica(f func(i int, r *ReplicaClient)) {
	var wg sync.WaitGroup
	wg.Add(m.replicaCount)
	for i := 0; i < m.replicaCount; i++ {
		go func(i int, r *ReplicaClient) {
			defer wg.Done()
			f(i, r)
		}(i, m.replicas[i])
	}
	wg.Wait()
}

func (m *Master) Ping(args *PingArgs, reply *GetResult) (err error) {
	reply.Value = args.Key
	return nil
}

func (m *Master) Recover() (err error) {
	entries, err := m.log.read()
	if err != nil {
		return
	}

	m.didSuicide = false
	for _, entry := range entries {
		switch entry.txId {
		case killedSelfMarker:
			m.didSuicide = true
			continue
		case firstRestartAfterSuicideMarker:
			m.didSuicide = false
			continue
		}

		switch entry.state {
		case Started:
			// abort
		case Prepared:
		case Committed:
		case Aborted:
		}
	}

	if m.didSuicide {
		m.log.writeSpecial(firstRestartAfterSuicideMarker)
	}
	return
}

func (m *Master) dieIf(actual MasterDeath, expected MasterDeath) {
	if !m.didSuicide && actual == expected {
		log.Println("Killing self as requested at", expected)
		m.log.writeSpecial(killedSelfMarker)
		os.Exit(1)
	}
}

func runMaster(replicaCount int) {
	if replicaCount <= 0 {
		log.Fatalln("Replica count must be greater than 0.")
	}

	master := NewMaster(replicaCount)
	err := master.Recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	server := rpc.NewServer()
	server.Register(master)
	log.Println("Master listening on port", MasterPort)
	http.ListenAndServe(MasterPort, server)
}
