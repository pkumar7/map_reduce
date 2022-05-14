package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		is_complete := execute_map_task(mapf)
		if is_complete {
			break
		}
	}
	time.Sleep(10)
	for {
		is_complete := execute_reduce_task(reducef)
		if is_complete {
			break
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
}

func execute_map_task(mapf func(string, string) []KeyValue) (is_complete bool) {
	var intermedeate_file_names []string
	filename, map_task_number := CallReturnTask("map_task")
	log.Printf("Procesing map file, ", filename)
	if filename == "" {
		log.Printf("No files available to process for map")
		return true
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		reduce_task_number := ihash(kv.Key)
		intermedeate_file_name := fmt.Sprintf("mr-%d-%d.json", map_task_number, reduce_task_number)
		intermedetate_file, _ := os.OpenFile(intermedeate_file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(intermedetate_file)
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot encode the content from map function for file %v", intermedetate_file)
		}
		if !contains(intermedeate_file_names, intermedeate_file_name) {
			intermedeate_file_names = append(intermedeate_file_names, intermedeate_file_name)
		}
		intermedetate_file.Close()
	}
	log.Printf("adding reduce inermedeate tasks, ", intermedeate_file_names)
	CallUpdateMapTaskCompletion(intermedeate_file_names, filename)
	return false
}

func execute_reduce_task(reducef func(string, []string) string) (is_complete bool) {
	filename, _ := CallReturnTask("reduce_task")
	log.Printf("Calling reduce task with filename", filename)
	if filename == "" {
		log.Printf("No files available to process for reduce")
		return true
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	dec := json.NewDecoder(file)
	kva := []KeyValue{}

	var reduce_task_key string
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
		reduce_task_key = kv.Key
	}
	reduce_key_number := ihash(reduce_task_key)
	oname := fmt.Sprintf("mr-out-%v", reduce_key_number)
	ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	CallUpdateReduceTaskCompletion(filename)
	return false
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func CallUpdateReduceTaskCompletion(reduce_file_name string) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.Reduce_file_name = reduce_file_name

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.CallUpdateReduceTaskCompletion", &args, &reply)
	if ok {
		// reply.Y should be 100.
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallUpdateMapTaskCompletion(filenames []string, map_file_name string) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.Intermedeate_file_names = filenames
	args.Map_file_name = map_file_name

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.CallUpdateMapTaskCompletion", &args, &reply)
	if ok {
		// reply.Y should be 100.
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallReturnTask(task_type string) (string, int) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	if task_type == "map_task" {
		args.Task_Type = "map_task"
	}

	if task_type == "reduce_task" {
		args.Task_Type = "reduce_task"
	}

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.ReturnTask" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.ReturnTask", &args, &reply)
	if ok {
		return reply.Filename, reply.TaskNumber
	}
	fmt.Printf("call failed!\n")
	return "", 0
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
