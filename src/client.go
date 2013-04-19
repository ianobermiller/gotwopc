package main

import (
	"fmt"
	"log"
	"net/rpc"
)

var _ = fmt.Errorf

type Client struct {
	rpcClient *rpc.Client
}

func NewClient() *Client {
	client, err := rpc.DialHTTP("tcp", "localhost"+MasterPort)
	if err != nil {
		log.Fatal("Dialing: ", err)
	}
	return &Client{client}
}

func (c *Client) Put(key string, value string) (err error) {
	var reply int
	err = c.rpcClient.Call("Master.Put", &KeyValueArgs{key, value}, &reply)
	if err != nil {
		log.Fatal("Client Put error: ", err)
		return
	}
	//fmt.Println("Put: ", key, " = ", value)
	return
}

func (c *Client) Get(key string) (value string, err error) {
	var reply GetResult
	err = c.rpcClient.Call("Master.Get", &KeyArgs{key}, &reply)
	if err != nil {
		log.Fatal("Client Get error: ", err)
		return
	}
	value = reply.Value
	//fmt.Println("Got: ", reply.Value)
	return
}

func (c *Client) Del(key string) (err error) {
	var reply int
	err = c.rpcClient.Call("Master.Del", &KeyArgs{key}, &reply)
	if err != nil {
		log.Fatal("Client Del error: ", err)
		return
	}
	//fmt.Println("Deleted: ", key)
	return
}
