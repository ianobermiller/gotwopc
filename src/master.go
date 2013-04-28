package main

import (
	"errors"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
)

var _ = errors.New

type Master struct {
	replicaCount int
	replicas     []*ReplicaClient
	log          *logger
}

type KeyValueArgs struct {
	Key   string
	Value string
}

type GetArgs struct {
	Key string
}

type GetTestArgs struct {
	Key        string
	ReplicaNum int
}

type KeyArgs struct {
	Key string
}

type GetResult struct {
	Value string
}

func NewMaster(replicaCount int) *Master {
	l := newLogger("log.master.txt")
	replicas := make([]*ReplicaClient, replicaCount)
	for i := 0; i < replicaCount; i++ {
		replicas[i] = NewReplicaClient(GetReplicaHost(i))
	}
	return &Master{replicaCount, replicas, l}
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

func (m *Master) Del(args *KeyArgs, _ *int) error {
	return nil
}

func (m *Master) Put(args *KeyValueArgs, _ *int) (err error) {
	log.Println("Master.Put is being called")
	for i := 0; i < m.replicaCount; i++ {
		success, err := m.replicas[i].TryPut(args.Key, args.Value, "")
		if err != nil {
			return err
		}
		if !*success {
			err = errors.New("Transaction aborted")
			return err
		}
	}
	return
}

func (m *Master) Ping(args *KeyArgs, reply *GetResult) (err error) {
	reply.Value = args.Key
	return nil
}

func runMaster(replicaCount int) {
	if replicaCount <= 0 {
		log.Fatalln("Replica count must be greater than 0.")
	}

	master := NewMaster(replicaCount)
	server := rpc.NewServer()
	server.Register(master)
	log.Println("Master listening on port", MasterPort)
	http.ListenAndServe(MasterPort, server)
}
