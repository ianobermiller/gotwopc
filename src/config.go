package main

import (
	"fmt"
)

const MasterPort = "localhost:7170"
const ReplicaPortStart = 7171

func GetReplicaHost(replicaNum int) string {
	return fmt.Sprintf("localhost:%v", ReplicaPortStart+replicaNum)
}

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
	NoOp Operation = iota
	PutOp
	DelOp
	RecoveryOp
)

func (s Operation) String() string {
	switch s {
	case PutOp:
		return "PUT"
	case DelOp:
		return "DEL"
	case RecoveryOp:
		return "RECOVERY"
	}
	return "INVALID"
}

func ParseOperation(s string) Operation {
	switch s {
	case "PUT":
		return PutOp
	case "DEL":
		return DelOp
	case "RECOVERY":
		return RecoveryOp
	}
	return NoOp
}

type ReplicaDeath int

const (
	ReplicaDontDie ReplicaDeath = iota

	// During mutation
	ReplicaDieBeforeProcessingMutateRequest
	ReplicaDieAfterLoggingPrepared

	// During commit
	ReplicaDieBeforeProcessingCommit
	ReplicaDieAfterDeletingFromTempStore
	ReplicaDieAfterLoggingCommitted
)

type MasterDeath int

const (
	MasterDontDie MasterDeath = iota
	MasterDieBeforeLoggingCommitted
	MasterDieAfterLoggingCommitted
)

var killedSelfMarker = "::justkilledself::"
var firstRestartAfterSuicideMarker = "::firstrestartaftersuicide::"
