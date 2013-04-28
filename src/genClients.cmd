go build tools\generateRpcClient.go
generateRpcClient.exe master.go > masterClient.go
generateRpcClient.exe replica.go > replicaClient.go
go build