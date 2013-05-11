taskkill /f /im src.exe
set GOMAXPROCS=2
go build && go test -test.v -gocheck.vv %*