package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"
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

type CommitArgs struct {
	TxId string
	Die  ReplicaDeath
}

type AbortArgs struct {
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
	didSuicide     bool
}

func NewReplica(num int) *Replica {
	l := newLogger(fmt.Sprintf("logs/replica%v.txt", num))
	return &Replica{
		num,
		newKeyValueStore(fmt.Sprintf("data/replica%v/committed", num)),
		newKeyValueStore(fmt.Sprintf("data/replica%v/temp", num)),
		make(map[string]*Tx),
		make(map[string]bool),
		l,
		false}
}

func (r *Replica) getTempStoreKey(txId string, key string) string {
	return txId + "__" + key
}

func (r *Replica) parseTempStoreKey(key string) (txId string, txKey string) {
	split := strings.Split(key, "__")
	return split[0], split[1]
}

func (r *Replica) TryPut(args *TxPutArgs, reply *ReplicaActionResult) (err error) {
	writeToTempStore := func() error { return r.tempStore.put(r.getTempStoreKey(args.TxId, args.Key), args.Value) }
	return r.tryMutate(args.Key, args.TxId, args.Die, PutOp, writeToTempStore, reply)
}

func (r *Replica) TryDel(args *TxDelArgs, reply *ReplicaActionResult) (err error) {
	return r.tryMutate(args.Key, args.TxId, args.Die, DelOp, nil, reply)
}

func (r *Replica) tryMutate(key string, txId string, die ReplicaDeath, op Operation, f func() error, reply *ReplicaActionResult) (err error) {
	r.dieIf(die, ReplicaDieBeforeProcessingMutateRequest)
	reply.Success = false

	r.txs[txId] = &Tx{txId, key, op, Started}

	if _, ok := r.lockedKeys[key]; ok {
		// Key is currently being modified, Abort
		log.Println("Received", op.String(), "for locked key:", key, "in tx:", txId, " Aborting")
		r.txs[txId].state = Aborted
		r.log.writeState(txId, Aborted)
		return nil
	}

	r.lockedKeys[key] = true

	if f != nil {
		err = f()
		if err != nil {
			log.Println("Unable to", op.String(), "uncommited val for transaction:", txId, "key:", key, ", Aborting")
			r.txs[txId].state = Aborted
			r.log.writeState(txId, Aborted)
			delete(r.lockedKeys, key)
			return
		}
	}

	r.txs[txId].state = Prepared
	r.log.writeOp(txId, Prepared, op, key)
	reply.Success = true

	r.dieIf(die, ReplicaDieAfterLoggingPrepared)

	return
}

func (r *Replica) Commit(args *CommitArgs, reply *ReplicaActionResult) (err error) {
	r.dieIf(args.Die, ReplicaDieBeforeProcessingCommit)

	reply.Success = false

	txId := args.TxId

	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Error! We've never heard of this transaction
		log.Println("Received commit for unknown transaction:", txId)
		return errors.New(fmt.Sprint("Received commit for unknown transaction:", txId))
	}

	_, keyLocked := r.lockedKeys[tx.key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		log.Println("Received commit for transaction with unlocked key:", txId)
	}

	switch tx.state {
	case Prepared:
		err = r.commitTx(txId, tx.op, tx.key, args.Die)
	default:
		log.Println("Received commit for transaction in state ", tx.state.String())
	}

	if err == nil {
		reply.Success = true
	}
	return
}

func (r *Replica) commitTx(txId string, op Operation, key string, die ReplicaDeath) (err error) {
	delete(r.lockedKeys, key)

	switch op {
	case PutOp:
		val, err := r.tempStore.get(r.getTempStoreKey(txId, key))
		if err != nil {
			return errors.New(fmt.Sprint("Unable to find val for uncommitted tx:", txId, "key:", key))
		}
		err = r.committedStore.put(key, val)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to put committed val for tx:", txId, "key:", key))
		}
	case DelOp:
		err = r.committedStore.del(key)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to commit del val for tx:", txId, "key:", key))
		}
	}

	r.log.writeState(txId, Committed)
	delete(r.txs, txId)

	// Delete the temp data only after committed, in case we crash after deleting, but before committing
	if op == PutOp {
		err = r.tempStore.del(r.getTempStoreKey(txId, key))
		r.dieIf(die, ReplicaDieAfterDeletingFromTempStore)
		if err != nil {
			fmt.Println("Unable to del committed val for tx:", txId, "key:", key)
		}
	}

	r.dieIf(die, ReplicaDieAfterLoggingCommitted)
	return nil
}

func (r *Replica) Abort(args *AbortArgs, reply *ReplicaActionResult) (err error) {
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

	switch tx.state {
	case Prepared:
		r.abortTx(txId, tx.op, tx.key)
	default:
		log.Println("Received abort for transaction in state ", tx.state.String())
	}

	reply.Success = true
	return nil
}

func (r *Replica) abortTx(txId string, op Operation, key string) {
	delete(r.lockedKeys, key)

	switch op {
	case PutOp:
		// We no longer need the temp stored value
		err := r.tempStore.del(r.getTempStoreKey(txId, key))
		if err != nil {
			fmt.Println("Unable to del val for uncommitted tx:", txId, "key:", key)
		}
		//case DelOp:
		// nothing to undo here
	}

	r.log.writeState(txId, Aborted)
	delete(r.txs, txId)
}

func (r *Replica) Get(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	val, err := r.committedStore.get(args.Key)
	if err != nil {
		return
	}
	reply.Value = val
	return
}

func (r *Replica) Ping(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	reply.Value = args.Key
	return nil
}

func (r *Replica) recover() (err error) {
	entries, err := r.log.read()
	if err != nil {
		return
	}

	r.didSuicide = false
	for _, entry := range entries {
		switch entry.txId {
		case killedSelfMarker:
			r.didSuicide = true
			continue
		case firstRestartAfterSuicideMarker:
			r.didSuicide = false
			continue
		}

		if entry.state == Prepared {
			entry.state = r.getStatus(entry.txId)
			switch entry.state {
			case Aborted:
				log.Println("Aborting transaction during recovery: ", entry.txId, entry.key)
				r.abortTx(entry.txId, RecoveryOp, entry.key)
			case Committed:
				log.Println("Committing transaction during recovery: ", entry.txId, entry.key)
				r.commitTx(entry.txId, RecoveryOp, entry.key, ReplicaDontDie)
			}
		}

		switch entry.state {
		case Started:
		case Prepared:
			// abort
		case Committed:
			r.txs[entry.txId] = &Tx{entry.txId, entry.key, entry.op, Committed}
		case Aborted:
			r.txs[entry.txId] = &Tx{entry.txId, entry.key, entry.op, Aborted}
		}
	}

	err = r.cleanUpTempStore()
	if err != nil {
		return
	}

	if r.didSuicide {
		r.log.writeSpecial(firstRestartAfterSuicideMarker)
	}
	return
}

func (r *Replica) cleanUpTempStore() (err error) {
	keys, err := r.tempStore.list()
	if err != nil {
		return
	}

	for _, key := range keys {
		txId, _ := r.parseTempStoreKey(key)
		tx, ok := r.txs[txId]
		if !ok || tx.state != Prepared {
			println("Cleaning up temp key ", key)
			err = r.tempStore.del(key)
			if err != nil {
				return
			}
		}
	}
	return nil
}

// getStatus is only used during recovery to check the status from the Master
func (r *Replica) getStatus(txId string) TxState {
	client := NewMasterClient(MasterPort)
	for {
		state, err := client.Status(txId)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return *state
	}

	return NoState
}

func runReplica(num int) {
	replica := NewReplica(num)
	err := replica.recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	server := rpc.NewServer()
	server.Register(replica)
	log.Println("Replica", num, "listening on port", ReplicaPortStart+num)
	http.ListenAndServe(GetReplicaHost(num), server)
}

func (r *Replica) dieIf(actual ReplicaDeath, expected ReplicaDeath) {
	if !r.didSuicide && actual == expected {
		log.Println("Killing self as requested at", expected)
		r.log.writeSpecial(killedSelfMarker)
		os.Exit(1)
	}
}
