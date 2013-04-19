package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
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

func startMaster(close chan int) {
	master := new(Master)
	server := rpc.NewServer()
	server.Register(master)
	l, e := net.Listen("tcp", MasterPort)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, server)
	go func() {
		<-close
		println("closing master")
		l.Close()
		close <- 1
	}()
	log.Println("Master listening on port ", MasterPort)
}
