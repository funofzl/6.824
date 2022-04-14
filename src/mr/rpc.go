/*
 * @Author: lxk
 * @Date: 2022-02-22 21:50:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-02-25 10:48:32
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
// 这里不需要参数 没什么好说的 很多调用只需要类型 不需要参数的
type TaskReqArgs struct {
	//需要worker的sockname
	Sockname string
}

type TaskReqReply struct {
	HasTask    bool     // 如果没有任务了 就标记一下false
	Rule       RuleType // 告知是mapper 还是 reducers
	WorkerIdx  int      // workerIdx决定socket文件
	TargetPos  string   // 如果是mapper 目标文件是哪个(reducer的文件名固定 所以不需要)
	ReducerIdx int      // 如果是reducer 需要reducer idx用来判断需要reduce哪些文件
	ReduceNum  int      // 所有reducer数目 mapper需要
}

// 2. worker 向coordinator "请求" RecuceNum --------------
type ReduceNumReqArgs struct {
}

type ReduceNumReqReply struct {
	ReduceNum int
}

//3. coordinator 向worker "询问" 是否成功  ----------------
type WorkIsDoneArgs struct {
}

type WorkIsDoneReply struct {
	IsDone bool
	Rule   RuleType
}

//4. IssueByCoordinator 向worker 发布任务 -----------------
type IssueByCoordinatorArgs struct {
	Close      bool
	Rule       RuleType
	TargetPos  string
	ReducerIdx int // reducer int 用来判断需要reduce哪些文件
}

type IssueByCoordinatorReply struct {
	OK bool
}

// 5. 完成之后需要向coordinator 提交完成信号 ---------------
type DoneCommitArgs struct {
	Done      bool
	Rule      RuleType
	WorkerIdx int
}

type DoneCommitReply struct {
}

// 6. ReduceBegin
type ReduceBeginArgs struct {
	MapperIdx []int
}

type ReduceBeginReply struct {
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
func workerSock(workerIdx int) string {
	s := "/var/tmp/824-mr-worker-"
	s += strconv.Itoa(workerIdx)
	return s
}
