package main

import (
	"fmt"
)

const MasterPort = "localhost:7170"
const ReplicaPortStart = 7171

type TxState int

const (
	NoState TxState = iota
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

func ParseTxState(s string) TxState {
	switch s {
	case "STARTED":
		return Started
	case "PREPARED":
		return Prepared
	case "COMMITTED":
		return Committed
	case "ABORTED":
		return Aborted
	}
	return NoState
}

type Operation int

const (
	NoOp  Operation = iota
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

func ParseOperation(s string) Operation {
	switch s {
	case "PUT":
		return PutOp
	case "DEL":
		return DelOp
	}
	return NoOp
}

type ReplicaDeath int

const (
	ReplicaDontDie ReplicaDeath = iota

	// During mutation
	ReplicaDieBeforeProcessingMutateRequest
	ReplicaDieAfterAbortingDueToLock
	ReplicaDieAfterWritingToTempStore
	ReplicaDieAfterLoggingPrepared

	// During commit
	ReplicaDieBeforeProcessingCommit
	ReplicaDieAfterWritingToCommittedStore
	ReplicaDieAfterDeletingFromTempStore
	ReplicaDieAfterDeletingFromComittedStore
	ReplicaDieAfterLoggingCommitted
)

func GetReplicaHost(replicaNum int) string {
	return fmt.Sprintf("localhost:%v", ReplicaPortStart+replicaNum)
}
