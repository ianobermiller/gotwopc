package main

import (
	"fmt"
)

const MasterPort = "localhost:7170"
const ReplicaPortStart = 7171

type TxState int

const (
	_ TxState = iota
	Started
	Prepared
	Committed
	Aborted
)

func (s TxState) String() string {
	switch s {
	case Started:
		return "STARTED"
	case Prepared:
		return "PREPARED"
	case Committed:
		return "COMMITTED"
	case Aborted:
		return "ABORTED"
	}
	return "INVALID"
}

type Operation int

const (
	PutOp Operation = iota
	DelOp
)

func (s Operation) String() string {
	switch s {
	case PutOp:
		return "PUT"
	case DelOp:
		return "DEL"
	}
	return "INVALID"
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

func GetReplicaHost(replicaNum int) string {
	return fmt.Sprintf("localhost:%v", ReplicaPortStart+replicaNum)
}
