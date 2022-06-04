/*
 * @Author: lxk
 * @Date: 2022-02-22 21:50:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-06-03 13:46:02
 */
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
// #############  正文开始  ##################//
// 1. worker 向coordinator "请求" task ---------------------
type TaskReqArgs struct {
	WorkerId string
}

type TaskReqReply struct {
	TaskInfo
}

// 2. 完成之后需要向coordinator 提交完成信号 ---------------
type DoneCommitArgs struct {
	Done      bool
	WorkerId  string   // who
	Role      RoleType // what
	PartIndex int      // which
}

type DoneCommitReply struct {
}

// 未完待续...

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// 每个worker启动时都需要
func workerSock(workerId string) string {
	s := "/var/tmp/824-mr-worker-"
	s += workerId
	return s
}
