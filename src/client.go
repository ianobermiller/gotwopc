package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type Client struct {
	rpcClient *rpc.Client
}

func NewClient() *Client {
	client, err := rpc.DialHTTP("tcp", "localhost"+MasterPort)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	return &Client{client}
}

func (c *Client) Get(key string) (value string, err error) {
	var reply GetResult
	err = c.rpcClient.Call("Get", &KeyArgs{key}, &reply)
	if err != nil {
		log.Fatal("Get error: ", err)
		return
	}
	value = reply.Value
	fmt.Println("Got: ", reply.Value)
	return
}
