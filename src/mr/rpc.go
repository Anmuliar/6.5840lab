package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	MapTask		TaskType = 0
	ReduceTask  TaskType = 1
	NoTask 		TaskType = 2
) // define the task type of the mapreduce task

type TaskArgs struct {
	WorkerId 	int // specific id for worker, created by hash
}

type TaskReply struct {
	TaskDesc	TaskType
	TaskId 		int
	InputFile 	string
	ReduceN		int
	MapN 		int  //
	ReduceIndex int  // for reduce phase, use it to mark which file to read
}

type TaskCompleteArgs struct {
	TaskId 		int
	TaskDesc 	TaskType
	WorkerId 	int
}

type TaskCompleteReply struct {
	Success bool
}

type RegisterWorkerArgs struct {
    // Empty, worker is just requesting an ID
}

type RegisterWorkerReply struct {
    WorkerId int
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
