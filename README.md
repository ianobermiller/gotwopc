## Two-Phase Commit in Go

Build status: [![Build Status](http://goci.me/project/image/github.com/ianobermiller/gotwopc)](http://goci.me/project/github.com/ianobermiller/gotwopc)

Implement the two-phase commit protocol around a replicated key-value store in Go.

To get started:

1. Install Go v1 http://golang.org/doc/install
2. Install Bazaar http://wiki.bazaar.canonical.com/Download (needed for `gocheck`)
3. `git clone git://github.com/ianobermiller/gotwopc.git`
4. `cd gotwopc`
5. `go get` to install dependencies
6. `go test`

Some notes:

* Persistent storage uses the filesystem, with the keys just being filenames
* Each replica has a directory under `data` with two dirs, `temp` for uncommitted data, and `committed` for committed data
* Each replica and the master have a log file under `logs`
* Logs are CSVs, with each entry having the format `TransactionId,STATE,OPERATION,Key` (some entries don't use all the fields, so they get default values to keep things simple)

TODO:

* Trim the log
* Clean up junk in temp data