package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X                       int
	Task_Type               string
	Intermedeate_file_names []string
	Map_file_name           string
	Reduce_file_name        string
}

type TaskStatusArgs struct {
	Status bool
}

type ExampleReply struct {
	Y          int
	Filename   string
	TaskNumber int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/map-reduce-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
