Go to the main directory:

cd ~/map_reduce

cd src/main

rm mr-out*


go build -race -buildmode=plugin ../mrapps/wc.go

go run -race mrcoordinator.go pg-*.txt

For Testing, can run the program with hello world inputs:

go run -race mrcoordinator.go pg-1-*.txt

