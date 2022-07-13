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

type MapReduceError int

func (e MapReduceError) Error() string {
	return strconv.Itoa(int(e))
}

const (
	MapReduceOk                       = MapReduceError(0)
	ErrTaskNotFound    MapReduceError = iota
	ErrUnmatchedWorker MapReduceError = iota
	ErrRpcFailed       MapReduceError = iota
	ErrTaskType        MapReduceError = iota
)

type GetTaskArgs struct {
	Whoami uint64
}

type GetTaskReply struct {
	Task Task
}

type SubmitTaskArgs struct {
	Whoami  uint64
	TaskId  uint64
	Results []string
}

type SubmitTaskReply struct {
	Err MapReduceError
}

type GetParamsArgs struct {
	Key string
}

type GetParamsReply struct {
	Value int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
