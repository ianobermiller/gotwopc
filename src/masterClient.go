
package main

import (
	"log"
	"net/rpc"
)

type MasterClient struct {
	host      string
	rpcClient *rpc.Client
}

func NewMasterClient(host string) *MasterClient {
	client := &MasterClient{host, nil}
	client.tryConnect()
	return client
}

func (c *MasterClient) tryConnect() (err error) {
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

func (c *MasterClient) call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	err = c.rpcClient.Call(serviceMethod, args, reply)
	if err == rpc.ErrShutdown {
		c.rpcClient = nil
	}
	return
}

func (c *MasterClient) Get(key string) (Value *string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply GetResult
	err = c.call("Master.Get", &GetArgs{ key }, &reply)
	if err != nil {
		log.Println("MasterClient.Get:", err)
		return
	}
	
	Value = &reply.Value
	
	return
}

func (c *MasterClient) GetTest(key string, replicanum int) (Value *string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply GetResult
	err = c.call("Master.GetTest", &GetTestArgs{ key, replicanum }, &reply)
	if err != nil {
		log.Println("MasterClient.GetTest:", err)
		return
	}
	
	Value = &reply.Value
	
	return
}

func (c *MasterClient) Del(key string, replicadeaths []ReplicaDeath) (err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply int
	err = c.call("Master.Del", &DelArgs{ key, replicadeaths }, &reply)
	if err != nil {
		log.Println("MasterClient.Del:", err)
		return
	}
	
	return
}

func (c *MasterClient) Put(key string, value string, replicadeaths []ReplicaDeath) (err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply int
	err = c.call("Master.Put", &PutArgs{ key, value, replicadeaths }, &reply)
	if err != nil {
		log.Println("MasterClient.Put:", err)
		return
	}
	
	return
}

func (c *MasterClient) Ping(key string) (Value *string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply GetResult
	err = c.call("Master.Ping", &PingArgs{ key }, &reply)
	if err != nil {
		log.Println("MasterClient.Ping:", err)
		return
	}
	
	Value = &reply.Value
	
	return
}
