package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	workerID := os.Getpid()

	for {
		args := GetTaskArgs{WorkerId: workerID}
		reply := GetTaskReply{}
		call("Coordinator.GetTask", &args, &reply)

		task := reply.Task

		switch task.TaskType {
		case TaskMap:
			doMap(task, workerID, mapf)
		case TaskReduce:
			doReduce(task, workerID, reducef)
		case TaskWait:
			time.Sleep(1 * time.Second)
		case TaskDone:
			return
		}
	}

}

func doMap(task Task, workerID int, mapf func(string, string) []KeyValue) {
	fileName := task.Filename

	file, err := os.Open(fileName)

	if err != nil {
		log.Printf("cannot load file %v", fileName)
		return
	}

	defer file.Close()

	content, err := io.ReadAll(file)

	if err != nil {
		log.Printf("cannot read file %v", fileName)
		return
	}

	kva := mapf(fileName, string(content))

	intermediate := make([][]KeyValue, task.NReduce)

	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Printf("cannot create intermediate file %v", i)
		}

		enc := json.NewEncoder(tmpFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("cannot encode intermediate file %v", i)
			}
		}

		tmpFile.Close()
		os.Rename(tmpFile.Name(), fileName)
	}

	reportTask(task, workerID)
}

func doReduce(task Task, workerID int, reducef func(string, []string) string) {
	reduceID := task.ReduceID
	mapTaskNum := task.MapTaskNum

	intermediate := make([]KeyValue, 0)

	for i := 0; i < mapTaskNum; i++ {
		mapfileName := fmt.Sprintf("mr-%d-%d", i, reduceID)
		tmpFile, err := os.Open(mapfileName)
		if err != nil {
			log.Printf("cannot open intermediate file %v", i)
			continue
		}

		dec := json.NewDecoder(tmpFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		tmpFile.Close()
	}

	sort.Sort(ByKey(intermediate))

	tmpFile, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		log.Printf("cannot create output file for reduce task %v", reduceID)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tmpFile.Close()
	fileName := fmt.Sprintf("mr-out-%d", reduceID)
	os.Rename(tmpFile.Name(), fileName)

	reportTask(task, workerID)
}

func reportTask(task Task, workerID int) ReportTaskReply {
	args := ReportTaskArgs{Task: task, WorkerId: workerID}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	log.Println(err)
	return false
}
