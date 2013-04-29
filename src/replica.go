package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
)

type Tx struct {
	id  string
	key string
	op  Operation
}

type TxPutArgs struct {
	Key   string
	Value string
	TxId  string
}

type TxDelArgs struct {
	Key  string
	TxId string
}

type TxArgs struct {
	TxId string
}

type ReplicaKeyArgs struct {
	Key string
}

type ReplicaGetResult struct {
	Value string
}

type ReplicaActionResult struct {
	Success bool
}

type Replica struct {
	num            int
	committedStore *keyValueStore
	tempStore      *keyValueStore
	txs            map[string]*Tx
	lockedKeys     map[string]bool
	log            *logger
}

func NewReplica(num int) *Replica {
	l := newLogger(fmt.Sprintf("log.replica%v.txt", num))
	return &Replica{
		num,
		newKeyValueStore(fmt.Sprintf("data/replica%v/committed", num)),
		newKeyValueStore(fmt.Sprintf("data/replica%v/temp", num)),
		make(map[string]*Tx),
		make(map[string]bool),
		l}
}

func (r *Replica) TryPut(args *TxPutArgs, reply *ReplicaActionResult) (err error) {
	reply.Success = false
	txId := args.TxId
	if _, ok := r.lockedKeys[args.Key]; ok {
		// Key is currently being modified, Abort
		fmt.Println("Received put for locked key:", args.Key, "in tx:", txId, ", Aborting")
		r.log.write(txId, Aborted)
		return nil
	}

	r.lockedKeys[args.Key] = true

	err = r.tempStore.put(args.Key, args.Value)
	if err != nil {
		fmt.Println("Unable to put uncommited val for transaction:", txId, "key:", args.Key, ", Aborting")
		r.log.write(txId, Aborted)
		r.lockedKeys[args.Key] = false
		return
	}

	r.txs[txId] = &Tx{txId, args.Key, PutOp}
	r.log.write(txId, Prepared, PutOp.String(), args.Key)
	reply.Success = true
	return
}

func (r *Replica) TryDel(args *TxDelArgs, reply *ReplicaActionResult) (err error) {
	reply.Success = false
	txId := args.TxId
	if _, ok := r.lockedKeys[args.Key]; ok {
		// Key is currently being modified, Abort
		fmt.Println("Received del for locked key:", args.Key, "in tx:", txId, ", Aborting")
		r.log.write(txId, Aborted)
		return nil
	}

	r.lockedKeys[args.Key] = true

	r.txs[txId] = &Tx{txId, args.Key, DelOp}
	r.log.write(txId, Prepared, DelOp.String(), args.Key)
	reply.Success = true
	return
}

func (r *Replica) Commit(args *TxArgs, reply *ReplicaActionResult) (err error) {
	reply.Success = false

	txId := args.TxId

	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Shouldn't happen, we've never heard of this transaction
		reply.Success = false
		return errors.New(fmt.Sprint("Received commit for unknown transaction:", txId))
	}

	_, keyLocked := r.lockedKeys[tx.key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		fmt.Println("Received commit for transaction with unlocked key:", txId)
	}

	r.lockedKeys[tx.key] = false

	switch tx.op {
	case PutOp:
		val, err := r.tempStore.get(tx.key)
		if err != nil {
			reply.Success = false
			return errors.New(fmt.Sprint("Unable to find val for uncommitted tx:", txId, "key:", tx.key))
		}
		err = r.committedStore.put(tx.key, val)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to put committed val for tx:", txId, "key:", tx.key))
		}
	case DelOp:
		err = r.committedStore.del(tx.key)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to del committed val for tx:", txId, "key:", tx.key))
		}
	}

	r.log.write(txId, Committed)
	delete(r.txs, txId)
	reply.Success = true
	return nil
}

func (r *Replica) Abort(args *TxArgs, reply *ReplicaActionResult) (err error) {
	reply.Success = false

	txId := args.TxId

	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Shouldn't happen, we've never heard of this transaction
		reply.Success = false
		return errors.New(fmt.Sprint("Received abort for unknown transaction:", txId))
	}

	_, keyLocked := r.lockedKeys[tx.key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		fmt.Println("Received abort for transaction with unlocked key:", txId)
	}

	r.lockedKeys[tx.key] = false

	switch tx.op {
	case PutOp:
		// We no longer need the temp stored value
		err := r.tempStore.del(tx.key)
		if err != nil {
			fmt.Println("Unable to del val for uncommitted tx:", txId, "key:", tx.key)
		}
		//case DelOp:
		// nothing to undo here
	}

	r.log.write(txId, Aborted)
	delete(r.txs, txId)
	reply.Success = true
	return nil
}

func (r *Replica) Get(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	val, err := r.committedStore.get(args.Key)
	if err != nil {
		return
	}
	reply.Value = val
	return
}

func (m *Replica) Ping(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	reply.Value = args.Key
	return nil
}

func runReplica(num int) {
	replica := NewReplica(num)
	server := rpc.NewServer()
	server.Register(replica)
	log.Println("Replica", num, "listening on port", ReplicaPortStart+num)
	http.ListenAndServe(GetReplicaHost(num), server)
}
