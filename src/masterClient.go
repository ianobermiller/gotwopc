
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

func (c *MasterClient) Get(key string) (Value *string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply GetResult
	err = c.rpcClient.Call("Master.Get", &KeyArgs{ key }, &reply)
	if err != nil {
		log.Println("MasterClient.Get:", err)
		return
	}
	
	Value = &reply.Value
	
	return
}

func (c *MasterClient) Del(key string) (err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply int
	err = c.rpcClient.Call("Master.Del", &KeyArgs{ key }, &reply)
	if err != nil {
		log.Println("MasterClient.Del:", err)
		return
	}
	
	return
}

func (c *MasterClient) Put(key string, value string) (err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply int
	err = c.rpcClient.Call("Master.Put", &KeyValueArgs{ key, value }, &reply)
	if err != nil {
		log.Println("MasterClient.Put:", err)
		return
	}
	
	return
}
