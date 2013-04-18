package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type Master struct {
}

type KeyValueArgs struct {
	Key   string
	Value string
}

type KeyArgs struct {
	Key string
}

type GetResult struct {
	Result string
}

func (m *Master) Get(args *KeyArgs, reply *GetResult) error {
	reply = &GetResult{"hello"}
	return nil
}

func (m *Master) Del(args *KeyArgs, _ *int) error {
	return nil
}

func (m *Master) Put(args *KeyValueArgs, _ *int) error {
	return nil
}

func startMaster() {
	master := new(Master)
	rpc.Register(master)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":7170")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go http.Serve(l, nil)
	log.Println("Master listening on port 7170")
	wg.Wait()
}
