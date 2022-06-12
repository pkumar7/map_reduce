package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Task map[string]interface{}

type Coordinator struct {
	// Your definitions here.
	map_tasks    []Task
	reduce_tasks []Task
	Lock         *sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReturnTask(args *TaskRequestArgs, reply *TaskReplyArgs) error {
	if args.Task_Type == "map_task" {
		for _, task := range c.map_tasks {
			if task["is_processed"] == true {
				continue
			}
			c.Lock.Lock()
			task["is_processed"] = true
			c.Lock.Unlock()
			reply.Filename = task["filename"].(string)
			reply.TaskNumber = task["task_number"].(int)
			return nil
		}
	}
	if args.Task_Type == "reduce_task" {
		for _, task := range c.reduce_tasks {
			if task["is_processed"] == true {
				continue
			}
			c.Lock.Lock()
			task["is_processed"] = true
			c.Lock.Unlock()
			reply.Filename = task["filename"].(string)
			return nil
		}
	}
	return nil
}

func (c *Coordinator) CallUpdateReduceTaskCompletion(args *TaskRequestArgs, reply *TaskReplyArgs) error {
	c.Lock.Lock()
	for _, task := range c.reduce_tasks {
		if args.Reduce_file_name == task["filename"] {
			task["is_completed"] = true
		}
	}
	c.Lock.Unlock()
	return nil
}

func (c *Coordinator) CallUpdateMapTaskCompletion(args *TaskRequestArgs, reply *TaskReplyArgs) error {
	c.Lock.Lock()
	for _, file_name := range args.Intermedeate_file_names {
		task_details := Task{"filename": file_name, "is_processed": false, "is_completed": false}
		c.reduce_tasks = append(c.reduce_tasks, task_details)
	}
	for _, task := range c.map_tasks {
		if args.Map_file_name == task["filename"] {
			task["is_completed"] = true
		}
	}
	c.Lock.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	for _, task := range c.map_tasks {
		if task["is_completed"] == false {
			return false
		}
	}
	for _, task := range c.reduce_tasks {
		if task["is_completed"] == false {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	for index, filename := range files {
		task_details := Task{"filename": filename, "is_processed": false,
			"is_completed": false, "task_number": index}
		c.map_tasks = append(c.map_tasks, task_details)
	}
	c.Lock = &sync.RWMutex{}
	c.server()
	return &c
}
