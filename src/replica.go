package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Replica struct {
}

func startReplica(num int) {
	replica := new(Replica)
	rpc.Register(replica)
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", fmt.Sprintf(":%v", ReplicaPortStart+num))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(listener, nil)
	log.Println("Replica ", num, " listening on port ", ReplicaPortStart+num)
}
