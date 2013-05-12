
package main

import (
	"log"
	"net"
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

func (c *ReplicaClient) call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	err = c.rpcClient.Call(serviceMethod, args, reply)
	_, isNetOpError := err.(*net.OpError)
	if err == rpc.ErrShutdown || isNetOpError {
		c.rpcClient = nil
	}
	return
}

func (c *ReplicaClient) TryPut(key string, value string, txid string, die ReplicaDeath) (Success *bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.TryPut", &TxPutArgs{ key, value, txid, die }, &reply)
	if err != nil {
		log.Println("ReplicaClient.TryPut:", err)
		return
	}
	
	Success = &reply.Success
	
	return
}

func (c *ReplicaClient) TryDel(key string, txid string, die ReplicaDeath) (Success *bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.TryDel", &TxDelArgs{ key, txid, die }, &reply)
	if err != nil {
		log.Println("ReplicaClient.TryDel:", err)
		return
	}
	
	Success = &reply.Success
	
	return
}

func (c *ReplicaClient) Commit(txid string, die ReplicaDeath) (Success *bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.Commit", &CommitArgs{ txid, die }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Commit:", err)
		return
	}
	
	Success = &reply.Success
	
	return
}

func (c *ReplicaClient) Abort(txid string) (Success *bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.Abort", &AbortArgs{ txid }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Abort:", err)
		return
	}
	
	Success = &reply.Success
	
	return
}

func (c *ReplicaClient) Get(key string) (Value *string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaGetResult
	err = c.call("Replica.Get", &ReplicaKeyArgs{ key }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Get:", err)
		return
	}
	
	Value = &reply.Value
	
	return
}

func (c *ReplicaClient) Ping(key string) (Value *string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaGetResult
	err = c.call("Replica.Ping", &ReplicaKeyArgs{ key }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Ping:", err)
		return
	}
	
	Value = &reply.Value
	
	return
}
