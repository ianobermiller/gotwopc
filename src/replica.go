package main

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
)

type Tx struct {
	id  string
	key string
	val string
	op  Operation
}

type TxPutArgs struct {
	Key   string
	Value string
	TxId  string
}

type TxArgs struct {
	TxId string
}

type ReplicaKeyArgs struct {
	Key string
}

type ReplicaGetResult struct {
	Value string
}

type ReplicaActionResult struct {
	Success bool
}

type Replica struct {
	store *keyValueStore
	txs   map[string]Tx
}

// func (r *Replica) TryPut/Del(args *TxPutArgs, reply *bool) (err error)
//      if request in flight for key, log and return false
//      else log and return true

// func (r *Replica) Rollback(args* TxArgs, reply *bool) (err error)
//      undo tx, log, and return true

// func (r *Replica) Commit(args* TxArgs, reply *bool) (err error)
//      write tx to committed storage, log, and return true

func (r *Replica) TryPut(args *TxPutArgs, reply *ReplicaActionResult) (err error) {
	err = r.store.put(args.Key, args.Value)
	if err != nil {
		reply.Success = false
		return
	}
	reply.Success = true
	return
}

func (r *Replica) Get(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	val, err := r.store.get(args.Key)
	if err != nil {
		return
	}
	reply.Value = val
	return
}

func (m *Replica) Ping(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	reply.Value = args.Key
	return nil
}

func runReplica(num int) {
	replica := &Replica{newKeyValueStore(fmt.Sprintf("replica%v", num)), make(map[string]Tx)}
	server := rpc.NewServer()
	server.Register(replica)
	log.Println("Replica", num, "listening on port", ReplicaPortStart+num)
	http.ListenAndServe(GetReplicaHost(num), server)
}
