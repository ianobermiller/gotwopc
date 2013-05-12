taskkill /f /im src.exe
set GOMAXPROCS=2
go build && go test -test.v -gocheck.vv %*

@echo off
rem To run only a single test:
rem -gocheck.f TestPutGetDelFromMaster