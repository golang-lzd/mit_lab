go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
rm middle-*
rm log*
go run -race mrcoordinator.go pg-*.txt

