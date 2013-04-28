package main

import (
	"fmt"
	flag "github.com/ogier/pflag"
	"log"
	"strconv"
)

func main() {
	isMaster := flag.BoolP("master", "m", false, "start the master process")
	replicaCount := flag.IntP("replicaCount", "n", 0, "replica count for master")
	isReplica := flag.BoolP("replica", "r", false, "start a replica process")
	replicaNumber := flag.IntP("replicaIndex", "i", 0, "replica index to run, starting at 0")
	flag.Parse()

	log.SetFlags(0) //log.Ltime | log.Lmicroseconds)

	switch {
	case *isMaster:
		log.SetPrefix("M  ")
		runMaster(*replicaCount)
	case *isReplica:
		log.SetPrefix(fmt.Sprint("R", strconv.Itoa(*replicaNumber), " "))
		runReplica(*replicaNumber)
	default:
		flag.Usage()
	}
}
