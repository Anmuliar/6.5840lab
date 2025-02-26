package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "math/rand"
import "fmt"

type Task struct {
	TaskDesc 	TaskType
	TaskFlag 	int   		// 0 -> todo; 1 -> ing; 2 -> done
	InputFile   string		// which file to do word count
	WorkerId	int 		// assigned to which worker
	ReduceId 	int			// reduce task mark
	StartTime 	time.Time
}

type Coordinator struct {
	
	mapTasks 	[]Task
	reduceTasks []Task
	nReduce 	int
	nMap		int
	phase 		TaskType 	

	// lock
	mu 			sync.Mutex
	// Worker management 
	workerIds     map[int]bool    // Track assigned worker IDs
    lastWorkerId  int             // Last assigned worker ID
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
func (c *Coordinator) AssignRandomWorkerId() int{
	fmt.Println("enter")
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("write")
	for {
		workerId := rand.Intn(100000) + 1
		fmt.Println(workerId)
		if !c.workerIds[workerId] {
			c.workerIds[workerId] = true
			return workerId
		}
	}

	
	return -1
}
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	fmt.Println("Recieved.")
	workerId := c.AssignRandomWorkerId()
	reply.WorkerId = workerId
	return nil
}

func (c *Coordinator) RequireTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == MapTask {
		for i, task := range c.mapTasks {
			if task.TaskFlag == 0 {
				task.TaskFlag = 1
				task.StartTime = time.Now()
				
				fmt.Println("Map task %v assigned.",i)
				reply.TaskDesc = c.phase
				reply.InputFile = task.InputFile
				reply.ReduceN = c.nReduce
				reply.TaskId = i
				return nil
			}
		}

		if c.checkAllTasksDone() {
			fmt.Println("Enter reduce phase.")
			c.phase = ReduceTask
		} else {
			reply.TaskDesc = NoTask
		}
	}
	if c.phase == ReduceTask {
		for i, task := range c.reduceTasks {
			if task.TaskFlag == 0 {
				task.TaskFlag = 1
				task.StartTime = time.Now()
				
				fmt.Println("Reduce task %v assigned.",i)
				reply.TaskDesc = c.phase
				reply.ReduceIndex = i
				reply.TaskId = i
				reply.MapN = c.nMap
				return nil

			}
		} 
	}
	reply.TaskDesc = NoTask
	return nil
}

func (c *Coordinator) DoneTask(args *TaskCompleteArgs, reply * TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if (args.TaskDesc == MapTask && args.TaskId < c.nMap) {
		c.mapTasks[args.TaskId].TaskFlag = 2
	} else if args.TaskDesc == ReduceTask && args.TaskId < c.nReduce {
		c.reduceTasks[args.TaskId].TaskFlag = 2
	}
	reply.Success = true
	
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
	// fmt.Println("!!!!")
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Println(c.phase)
	if c.phase != ReduceTask {
		return false
	}

	for _, task := range c.reduceTasks {
		// fmt.Println("%d %d",i,task.TaskFlag)
		if task.TaskFlag != 2 {
			return false
		}
	}
	// fmt.Println("Done")
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nMap: 	 len(files),
		phase: 	 MapTask,
		workerIds: make(map[int]bool),
	}
	for _, file := range files {
		c.mapTasks = append(c.mapTasks, Task{
			TaskDesc: MapTask,
			TaskFlag: 0,
			InputFile: file,
			WorkerId: -1,
		})
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			TaskDesc: ReduceTask,
			TaskFlag: 0,
			ReduceId: i,
			WorkerId: -1,
		})
	}
	// Your code here.
	c.server()
	go c.TimeoutReseter()
	return &c
}


func (c *Coordinator) checkAllTasksDone() bool {
	for _, task := range c.mapTasks {
		if task.TaskFlag != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) TimeoutReseter() {
	for {
		time.Sleep(10 * time.Second)
		c.mu.Lock()
		for _, task := range c.mapTasks {
			if (task.TaskFlag == 1 && time.Since(task.StartTime) > 10 * time.Second) {
					task.TaskFlag = 0
			}
		}

		for _, task := range c.reduceTasks {
			if (task.TaskFlag == 1 && time.Since(task.StartTime) > 10 * time.Second) {
					task.TaskFlag = 0
			}
		}
		c.mu.Unlock()
	}
}