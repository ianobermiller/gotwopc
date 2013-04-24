
package main

import (
	"log"
	"net/rpc"
)

type ReplicaClient struct {
	host      string
	rpcClient *rpc.Client
}

func NewReplicaClient(host string) *ReplicaClient {
	client := &ReplicaClient{host, nil}
	client.tryConnect()
	return client
}

func (c *ReplicaClient) tryConnect() (err error) {
	if c.rpcClient != nil {
		return
	}

	rpcClient, err := rpc.DialHTTP("tcp", c.host)
	if err != nil {
		return
	}
	c.rpcClient = rpcClient
	return
}

func (c *ReplicaClient) Get(key string) (Value *string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaGetResult
	err = c.rpcClient.Call("Replica.Get", &ReplicaKeyArgs{ key }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Get:", err)
		return
	}
	
	Value = &reply.Value
	
	return
}
