package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	TaskMap    = "map"
	TaskReduce = "reduce"
	TaskWait   = "wait"
	TaskDone   = "done"
)

const (
	Pending    = "pending"
	InProgress = "in_progress"
	Completed  = "completed"
)

type Task struct {
	TaskID     int
	TaskType   string
	Filename   string
	NReduce    int
	Status     string
	WorkerId   int
	StartTime  time.Time
	ReduceID   int
	MapTaskNum int
}

type Coordinator struct {
	// Your definitions here.
	files        []string
	nReduce      int
	mapTasks     []Task
	reduceTasks  []Task
	mapTasksDone bool
	allTaskDone  bool
	mu           sync.Mutex
}

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	Task   Task
	TaskID int
}

type ReportTaskArgs struct {
	Task     Task
	WorkerId int
}

type ReportTaskReply struct {
	Ok bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
