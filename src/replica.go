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

func (r *Replica) Get(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	val, err := r.store.get(args.Key)
	if err != nil {
		return
	}
	reply.Value = val
	return
}

func runReplica(num int) {
	replica := &Replica{newKeyValueStore(fmt.Sprintf("replica%v", num)), make(map[string]Tx)}
	server := rpc.NewServer()
	server.Register(replica)
	log.Println("Replica ", num, " listening on port ", ReplicaPortStart+num)
	http.ListenAndServe(fmt.Sprintf(":%v", ReplicaPortStart+num), server)
}
