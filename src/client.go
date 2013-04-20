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
	client := &Client{nil}
	client.tryConnect()
	return client
}

func (c *Client) tryConnect() (err error) {
	if c.rpcClient != nil {
		return
	}

	rpcClient, err := rpc.DialHTTP("tcp", "localhost"+MasterPort)
	if err != nil {
		return
	}
	c.rpcClient = rpcClient
	return
}

func (c *Client) Put(key string, value string) (err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply int
	err = c.rpcClient.Call("Master.Put", &KeyValueArgs{key, value}, &reply)
	if err != nil {
		log.Fatal("Client Put error: ", err)
		return
	}
	return
}

func (c *Client) Get(key string) (value string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply GetResult
	err = c.rpcClient.Call("Master.Get", &KeyArgs{key}, &reply)
	if err != nil {
		return
	}
	value = reply.Value
	return
}

func (c *Client) Del(key string) (err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply int
	err = c.rpcClient.Call("Master.Del", &KeyArgs{key}, &reply)
	if err != nil {
		return
	}
	return
}

func (c *Client) Ping() (err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply string
	err = c.rpcClient.Call("Master.Ping", "hello", &reply)
	if err != nil {
		return
	}
	return
}
