package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
)

type Tx struct {
	id    string
	key   string
	op    Operation
	state TxState
}

type TxPutArgs struct {
	Key   string
	Value string
	TxId  string
	Die   ReplicaDeath
}

type TxDelArgs struct {
	Key  string
	TxId string
	Die  ReplicaDeath
}

type TxArgs struct {
	TxId string
	Die  ReplicaDeath
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

type ReplicaDeath int

const (
	ReplicaDontDie ReplicaDeath = iota
	ReplicaDieBeforeProcessingMutateRequest
	ReplicaDieAfterAbortingDueToLock
	ReplicaDieAfterWritingToTempStore
	ReplicaDieAfterLoggingPrepared

	ReplicaDieBeforeProcessingCommit
	ReplicaDieAfterWritingToCommittedStore
	ReplicaDieAfterDeletingFromTempStore
	ReplicaDieAfterDeletingFromComittedStore
	ReplicaDieAfterLoggingCommitted
)

func NewReplica(num int) *Replica {
	l := newLogger(fmt.Sprintf("logs/replica%v.txt", num))
	return &Replica{
		num,
		newKeyValueStore(fmt.Sprintf("data/replica%v/committed", num)),
		newKeyValueStore(fmt.Sprintf("data/replica%v/temp", num)),
		make(map[string]*Tx),
		make(map[string]bool),
		l}
}

func (r *Replica) TryPut(args *TxPutArgs, reply *ReplicaActionResult) (err error) {
	writeToTempStore := func() error { return r.tempStore.put(args.Key, args.Value) }
	return r.tryMutate(args.Key, args.TxId, args.Die, PutOp, writeToTempStore, reply)
}

func (r *Replica) TryDel(args *TxDelArgs, reply *ReplicaActionResult) (err error) {
	return r.tryMutate(args.Key, args.TxId, args.Die, DelOp, nil, reply)
}

func (r *Replica) tryMutate(key string, txId string, die ReplicaDeath, op Operation, f func() error, reply *ReplicaActionResult) (err error) {
	dieIf(die, ReplicaDieBeforeProcessingMutateRequest)
	reply.Success = false

	r.txs[txId] = &Tx{txId, key, op, Started}

	if _, ok := r.lockedKeys[key]; ok {
		// Key is currently being modified, Abort
		log.Println("Received", op.String(), "for locked key:", key, "in tx:", txId, " Aborting")
		r.txs[txId].state = Aborted
		r.log.write(txId, Aborted)
		dieIf(die, ReplicaDieAfterAbortingDueToLock)
		return nil
	}

	r.lockedKeys[key] = true

	if f != nil {
		err = f()
		if err != nil {
			log.Println("Unable to", op.String(), "uncommited val for transaction:", txId, "key:", key, ", Aborting")
			r.txs[txId].state = Aborted
			r.log.write(txId, Aborted)
			delete(r.lockedKeys, key)
			return
		}
	}

	dieIf(die, ReplicaDieAfterWritingToTempStore)

	r.txs[txId].state = Prepared
	r.log.write(txId, Prepared, op.String(), key)
	reply.Success = true

	dieIf(die, ReplicaDieAfterLoggingPrepared)

	return
}

func (r *Replica) Commit(args *TxArgs, reply *ReplicaActionResult) (err error) {
	dieIf(args.Die, ReplicaDieBeforeProcessingCommit)

	reply.Success = false

	txId := args.TxId

	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Just ignore, we've never heard of this transaction
		return errors.New(fmt.Sprint("Received commit for unknown transaction:", txId))
	}

	_, keyLocked := r.lockedKeys[tx.key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		log.Println("Received commit for transaction with unlocked key:", txId)
	}

	delete(r.lockedKeys, tx.key)

	switch tx.op {
	case PutOp:
		val, err := r.tempStore.get(tx.key)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to find val for uncommitted tx:", txId, "key:", tx.key))
		}
		err = r.committedStore.put(tx.key, val)
		dieIf(args.Die, ReplicaDieAfterWritingToCommittedStore)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to put committed val for tx:", txId, "key:", tx.key))
		}

		err = r.tempStore.del(tx.key)
		dieIf(args.Die, ReplicaDieAfterDeletingFromTempStore)
		if err != nil {
			fmt.Println("Unable to del committed val for tx:", txId, "key:", tx.key)
		}
	case DelOp:
		err = r.committedStore.del(tx.key)
		dieIf(args.Die, ReplicaDieAfterDeletingFromComittedStore)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to commit del val for tx:", txId, "key:", tx.key))
		}
	}

	r.log.write(txId, Committed)
	delete(r.txs, txId)
	reply.Success = true

	dieIf(args.Die, ReplicaDieAfterLoggingCommitted)
	return nil
}

func (r *Replica) Abort(args *TxArgs, reply *ReplicaActionResult) (err error) {
	reply.Success = false

	txId := args.TxId

	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Shouldn't happen, we've never heard of this transaction
		return errors.New(fmt.Sprint("Received abort for unknown transaction:", txId))
	}

	_, keyLocked := r.lockedKeys[tx.key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		log.Println("Received abort for transaction with unlocked key:", txId)
	}

	delete(r.lockedKeys, tx.key)

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

func dieIf(actual ReplicaDeath, expected ReplicaDeath) {
	if actual == expected {
		log.Println("Killing self as requested at", expected)
		os.Exit(1)
	}
}
