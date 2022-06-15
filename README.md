Go to the main directory:

```
cd ~/map_reduce

cd src/main

rm mr-out*
```

Build the latest code:

```
go build -race -buildmode=plugin ../mrapps/wc.go
```

Run the cordinator

```
go run -race mrcoordinator.go pg-*.txt
```

For Testing, can run the program with hello world inputs, smaller files:

```
go run -race mrcoordinator.go pg-1-*.txt
```

Open another tab, move to main directory and run the Worker for word count application:

```
go run -race mrworker.go wc.so
```


References -  https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf