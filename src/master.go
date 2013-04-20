package main

import (
	"log"
	"net/http"
	"net/rpc"
)

type Master struct {
	replicaCount int
	log          *logger
}

type KeyValueArgs struct {
	Key   string
	Value string
}

type KeyArgs struct {
	Key string
}

type GetResult struct {
	Value string
}

func (m *Master) Get(args *KeyArgs, reply *GetResult) error {
	reply.Value = "hello"
	return nil
}

func (m *Master) Del(args *KeyArgs, _ *int) error {
	return nil
}

func (m *Master) Put(args *KeyValueArgs, _ *int) error {
	return nil
}

func runMaster(replicaCount int) {
	l := newLogger("log.master.txt")
	master := &Master{replicaCount, l}
	server := rpc.NewServer()
	server.Register(master)
	master.log.write("wtf mate")
	log.Println("Master listening on port", MasterPort)
	http.ListenAndServe(MasterPort, server)
}
