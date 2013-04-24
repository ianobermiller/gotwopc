package main

const MasterPort = "localhost:7170"
const ReplicaPortStart = 7171
const ReplicaCount = 3

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
