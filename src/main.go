package main

import (
	flag "github.com/ogier/pflag"
)

func main() {
	isMaster := flag.BoolP("master", "m", false, "start the master process")
	replicaCount := flag.IntP("replicaCount", "n", 0, "replica count for master")
	isReplica := flag.BoolP("replica", "r", false, "start a replica process")
	replicaNumber := flag.IntP("replicaIndex", "i", 0, "replica index to run, starting at 0")
	flag.Parse()

	switch {
	case *isMaster:
		runMaster(*replicaCount)
	case *isReplica:
		runReplica(*replicaNumber)
	default:
		flag.Usage()
	}
}
