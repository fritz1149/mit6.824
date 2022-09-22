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

// Add your RPC definitions here.

type RegisterArgs struct{}

type RegisterReply struct {
	WorkerIndex int
	ReduceN int
}

type RequestArgs struct {
	WorkerIndex int
	RPCIndex int
	IsFinish bool
}

var(
	Map int = 0 //map task
	Reduce int = 1 //reduce task
	Idle int = 2 //no task
	Done int = 3 //should exit
)
type RequestReply struct {
	TaskState int
	Filename string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func CoordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
