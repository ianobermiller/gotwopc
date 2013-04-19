package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Replica struct {
	store *keyValueStore
}

type KeyArgs struct {
	Key string
}

type GetResult struct {
	Value string
}

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
