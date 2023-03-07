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
type MapReply struct {
	Filename string
	TaskNum int
}

type RpcArgs struct {
	WorkerId int
	// 0: 首次请求任务 1：请求的worker的map已经完成map，再次请求任务 2：请求的worker的Reduce已经完成，再次请求
	Status int
	// 已完成的map任务号
	MapTaskNum int
	// 已完成的reduce任务号
	ReduceTaskNum int
}

type RpcReply struct {

	Filename string
	ReduceFilenames []string
	// 0: 已经全部完成任务  1：Map未完成 2： Reduce未完成 -1:worker应该等待
	Status int
	MapTaskNum int
	ReduceTaskNum int
	ReduceCount int
}

type Dog struct {
	X int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
