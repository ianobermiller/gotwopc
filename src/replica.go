package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type TxPutArgs struct {
	Key   string
	Value string
	Tx    string
}

type TxArgs struct {
	Tx string
}

type Replica struct {
	store *keyValueStore
}

// func (r *Replica) TryPut/Del(args *TxPutArgs, reply *bool) (err error)
//      if request in flight for key, log and return false
//      else log and return true

// func (r *Replica) Rollback(args* TxArgs, reply *bool) (err error)
//      undo tx, log, and return true

// func (r *Replica) Commit(args* TxArgs, reply *bool) (err error)
//      write tx to committed storage, log, and return true

func (r *Replica) Get(args *KeyArgs, reply *GetResult) (err error) {
	val, err := r.store.get(args.Key)
	if err != nil {
		return
	}
	reply.Value = val
	return
}

func startReplica(num int) {
	replica := &Replica{newKeyValueStore(fmt.Sprintf("replica%v", num))}
	rpc.Register(replica)
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", fmt.Sprintf(":%v", ReplicaPortStart+num))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(listener, nil)
	log.Println("Replica ", num, " listening on port ", ReplicaPortStart+num)
}
