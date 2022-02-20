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
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	TASK_TYPE_IDLE = iota
	TASK_TYPE_MAP
	TASK_TYPE_REDUCE
)

type TaskArgs struct {
	TaskId int
	TaskType int
	Finished bool
	MapFile []string
}

type TaskReply struct {
	TaskId int
	TaskType int
	Filename string
	NReduce int
	MapFile []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
