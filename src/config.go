package main

import (
	"fmt"
)

const MasterPort = "localhost:7170"
const ReplicaPortStart = 7171

type TxState int

const (
	_ TxState = iota
	Pending
	Committed
	Aborted
)

type Operation int

const (
	PutOp Operation = iota
	DelOp
)

func GetReplicaHost(replicaNum int) string {
	return fmt.Sprintf("localhost:%v", ReplicaPortStart+replicaNum)
}
