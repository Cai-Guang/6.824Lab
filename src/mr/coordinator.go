package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.CheckTimeOut()

	// go func() {
	// 	if reply.TaskID != -1 {
	// 		log.Println("GetTask", args.WorkerId, reply.TaskID, c.mapTasks[reply.TaskID].Status)
	// 	}
	// }()

	if c.allTaskDone {
		reply.Task.TaskType = TaskDone
		return nil
	}

	if c.mapTasksDone {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == Pending {
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].WorkerId = args.WorkerId
				c.reduceTasks[i].StartTime = time.Now()

				reply.Task = c.reduceTasks[i]
				reply.TaskID = i
				return nil
			}
		}
		reply.Task.TaskType = TaskWait
		return nil
	}

	for i := range c.mapTasks {
		if c.mapTasks[i].Status == Pending {
			c.mapTasks[i].Status = InProgress
			c.mapTasks[i].WorkerId = args.WorkerId
			c.mapTasks[i].StartTime = time.Now()

			reply.Task = c.mapTasks[i]
			reply.TaskID = i
			return nil
		}
	}

	// // 检查是否所有 map 任务都完成了
	// c.CheckTaskDone()

	reply.Task.TaskType = TaskWait
	reply.TaskID = -1
	return nil
}

func (c *Coordinator) CheckTimeOut() {
	timeout := 10 * time.Second
	for i := range c.mapTasks {
		if c.mapTasks[i].Status == InProgress && time.Since(c.mapTasks[i].StartTime) > timeout {
			c.mapTasks[i].Status = Pending
			log.Println("CheckTimeOut map", i, c.mapTasks[i].Status)
			// c.mapTasks[i].WorkerId = 0
		}
	}
	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status == InProgress && time.Since(c.reduceTasks[i].StartTime) > timeout {
			c.reduceTasks[i].Status = Pending
			log.Println("CheckTimeOut reduce", i, c.reduceTasks[i].Status)
			// c.reduceTasks[i].WorkerId = 0
		}
	}
}

func (c *Coordinator) CheckTaskDone() {
	if c.mapTasksDone {
		allTaskDone := true
		for _, task := range c.reduceTasks {
			if task.Status != Completed {
				allTaskDone = false
				break
			}
		}
		c.allTaskDone = allTaskDone
	} else {
		mapTasksDone := true
		for _, task := range c.mapTasks {
			if task.Status != Completed {
				mapTasksDone = false
				break
			}
		}
		c.mapTasksDone = mapTasksDone
	}
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	log.Printf("ReportTask", args.Task.TaskID, args.Task.TaskType)
	c.mu.Lock()
	defer c.mu.Unlock()

	// get := func() {
	// 	if args.Task.TaskID != -1 {
	// 		log.Println("ReportTask", args.Task.TaskID, args.Task.TaskType, c.mapTasks[args.Task.TaskID].Status)
	// 	}
	// }

	// get()

	// defer get()

	if args.Task.TaskType == TaskMap {
		c.mapTasks[args.Task.TaskID].Status = Completed
	} else if args.Task.TaskType == TaskReduce {
		c.reduceTasks[args.Task.TaskID].Status = Completed
	} else {
		reply.Ok = false
		return nil
	}

	c.CheckTaskDone()
	reply.Ok = true
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.CheckTaskDone()

	return c.allTaskDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:        files,
		nReduce:      nReduce,
		mapTasks:     make([]Task, len(files)),
		reduceTasks:  make([]Task, nReduce),
		mapTasksDone: false,
		allTaskDone:  false,
		mu:           sync.Mutex{},
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			TaskID:     i,
			Filename:   file,
			NReduce:    nReduce,
			TaskType:   TaskMap,
			Status:     Pending,
			WorkerId:   0,
			MapTaskNum: len(files),
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			TaskID:     i,
			NReduce:    nReduce,
			TaskType:   TaskReduce,
			Status:     Pending,
			MapTaskNum: len(files),
			ReduceID:   i,
		}
	}

	// Your code here.

	c.server()
	return &c
}
