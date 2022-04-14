/*
 * @Author: lxk
 * @Date: 2022-02-22 21:50:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-02-26 13:59:59
 */
package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const MaxReducerNum = 2

type WorkerInfo struct {
	rule      RuleType
	workerIdx int
	target    string // 如果是mapper
	reduceIdx int    // 如果是reducer
	// doneChan  chan bool //是否已经完成 worker主动告知会设置 然后判断可以退出goroutine
}

type task struct {
	target    string // mapper的工作
	reduceIdx int    // reduce的工作
}

type Coordinator struct {
	// Your definitions here.
	mutex           sync.Mutex          //用于更新下面的
	reduceNum       int                 // 就不要再关心mappers有多少个了 首先分配reducer 然后分配mapper
	originTaskCount int                 // 最初的文件数目
	tasks           []string            // 所有的target file name
	curWorkerCount  int                 // 当前已经启动的worker数目(用于分配workerIdx)
	workers         map[int]*WorkerInfo //从workerIdx到workINfo的映射
	idleWorker      []*WorkerInfo       // 使用slice当做队列 当然也可以使用channel

	//workerChan 用于将到来的workerworker传递到workers队列
	doneWorkerChan chan *WorkerInfo

	hasMapped []int // 存储已经完成map的mapperIdx表明哪些任务完成了
	// idleWorker存储已经完成的worker和没有task的worker 这样一个worker也可以完成所有工作

	// 发生意外的worker队列
	exceTask []*WorkerInfo

	MapperOkCount  int
	ReducedOkCount int

	doneChans map[int]chan bool //	workerIdx => doneChan
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Coordinator需要初始化
	c := Coordinator{
		reduceNum:       0,
		tasks:           files,
		originTaskCount: len(files),
		workers:         make(map[int]*WorkerInfo),
		idleWorker:      []*WorkerInfo{},
		curWorkerCount:  0,
		hasMapped:       []int{},
		doneWorkerChan:  make(chan *WorkerInfo),
		doneChans:       make(map[int]chan bool),
	}
	fmt.Printf("MakeCoordinator Succeed: there are [%d] tasks\n", len(c.tasks))
	// Your code here.
	// 启动监听服务
	c.server()
	// 开始分发任务(知道任务完成)
	go DiliveryTask(&c)
	return &c
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
	fmt.Println("Listening...")
	go http.Serve(l, nil)
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

func (c *Coordinator) TaskReq(args *TaskReqArgs, reply *TaskReqReply) error {
	c.mutex.Lock()
	var workerInfo WorkerInfo
	// 如果Reducer不够 那就Reducer
	if c.reduceNum < MaxReducerNum {
		*reply = TaskReqReply{
			HasTask:    true,
			Rule:       Reducer,
			WorkerIdx:  c.curWorkerCount,
			ReducerIdx: c.reduceNum,
			ReduceNum:  MaxReducerNum,
		}
		workerInfo = WorkerInfo{
			rule:      Reducer,
			workerIdx: c.curWorkerCount,
			reduceIdx: c.reduceNum,
		}
		c.reduceNum += 1
		c.curWorkerCount += 1
		c.workers[workerInfo.workerIdx] = &workerInfo
	} else {
		// Mapper
		if 0 == len(c.tasks) { // 没有任务了
			*reply = TaskReqReply{
				HasTask:   false,
				Rule:      Idle,
				WorkerIdx: c.curWorkerCount,
				ReduceNum: MaxReducerNum, // 万一以后需要用呢 免得再来一次
			}
			// 加入空闲队列
			workerInfo = WorkerInfo{
				rule:      Idle,
				workerIdx: c.curWorkerCount,
			}
			c.curWorkerCount++
			c.idleWorker = append(c.idleWorker, &workerInfo)
		} else {
			*reply = TaskReqReply{
				HasTask:   true,
				Rule:      Mapper,
				TargetPos: c.tasks[0],
				WorkerIdx: c.curWorkerCount,
				ReduceNum: MaxReducerNum,
			}
			workerInfo = WorkerInfo{
				rule:      Mapper,
				workerIdx: c.curWorkerCount,
				target:    c.tasks[0],
			}
			c.tasks = c.tasks[1:] // 使用slice做队列就是这样的
			c.curWorkerCount++
			c.workers[workerInfo.workerIdx] = &workerInfo
		}
	}
	c.doneChans[workerInfo.workerIdx] = make(chan bool)
	c.mutex.Unlock()
	OutWorkInfo(workerInfo)

	// 如果是mapper 应该马上启动 goroutine 检测超时
	if workerInfo.rule == Mapper {
		go communication(workerInfo.workerIdx, c)
	}
	return nil
}

func (c *Coordinator) ReduceNumReq(args *ReduceNumReqArgs, reply *ReduceNumReqReply) error {
	c.mutex.Lock()
	reply.ReduceNum = MaxReducerNum
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) DoneCommit(args *DoneCommitArgs, reply *DoneCommitReply) error {
	c.mutex.Lock()
	if args.Rule == Mapper {
		c.MapperOkCount++
		has := false
		for _, i := range c.hasMapped {
			if i == args.WorkerIdx {
				has = true
				break
			}
		}
		if !has {
			c.hasMapped = append(c.hasMapped, args.WorkerIdx)
		}
		if c.MapperOkCount == c.originTaskCount {
			// 通知所有reducer开始工作
			args := ReduceBeginArgs{
				MapperIdx: c.hasMapped,
			}
			reply := ReduceBeginReply{}
			for _, workerInfo := range c.workers {
				if workerInfo.rule == Reducer {
					c, err := rpc.DialHTTP("unix", workerSock(workerInfo.workerIdx))
					if err != nil {
						fmt.Printf("ReduceBegin DialHttp error: worker[%d]\n", workerInfo.workerIdx)
					}
					err = c.Call("Workerr.ReduceBegin", &args, &reply)
					if err != nil {
						fmt.Printf("ReduceBegin Call error: worker[%d]\n", workerInfo.workerIdx)
					}
				}
			}
		}
	} else if args.Rule == Reducer {
		c.ReducedOkCount++
	}
	c.mutex.Unlock()
	// TODO: reducer也需要做监控
	if args.Rule == Mapper {
		c.doneChans[args.WorkerIdx] <- true // 不用监听了 这里按道理不需要开启携程
	}
	// 将已经完成的worker 放进doneWorkerChan 然后有任务自行分配
	// OutWorkInfo(*c.workers[args.WorkerIdx])
	// c.mutex.Lock()
	c.doneWorkerChan <- c.workers[args.WorkerIdx]
	// c.mutex.Unlock()
	return nil
}

func communication(workerIdx int, c *Coordinator) {
	// 下面这种应该分类型 mapper应该等待
	// 至于reducers mappers都完成之后 在进行等待(同样是10s)
	select {
	case <-time.After(time.Second * 10):
		// 确保确实是没有完成(不管是什么原因)
		var args WorkIsDoneArgs
		var reply WorkIsDoneReply
		sockName := workerSock(workerIdx)
		socket, err := rpc.DialHTTP("unix", sockName)
		if err != nil {
			fmt.Printf("Can not connected to %s: %s\n", sockName, err)
			return
		}
		defer socket.Close()
		if err := socket.Call("Workerr.Done", &args, &reply); err != nil {
			fmt.Printf("ReqDone Failed: => worker[%s]\n", sockName)
		}
		if reply.IsDone {
			// Ok 完成了 这种情况基本不会出现 因为worker完成后都会调用DoneCommmit
			// 将worker放进doneWorkerChan 和上边DoneCommit一样的代码
			if c.workers[workerIdx].rule == Mapper {
				c.mutex.Lock()
				c.hasMapped = append(c.hasMapped, workerIdx)
				c.mutex.Unlock()
			}
			fmt.Printf("DoneCommit (with timeout): workerIdx[%d]\n", workerIdx)
			// 将已经完成的worker 放进doneWorkerChan 然后有任务自行分配
			c.doneWorkerChan <- c.workers[workerIdx]
			// fmt.Printf("Complete Succeed Acturally: workerIdx[%d]", workerIdx)
			return // 之后需要的话在开启协程就好了
		}
		// 将发生意外的workre加入c.execTask队列 直接存储全部信息 省得再构造结构体
		c.mutex.Lock()
		c.exceTask = append(c.exceTask, c.workers[workerIdx])
		// OutWorkInfo(*c.workers[workerIdx]) // 把失败情况输输出一下
		delete(c.workers, workerIdx) // 把不能用的worker删除
		c.mutex.Unlock()
	case <-c.doneChans[workerIdx]: // 这个chan在worker主动报告完成之后就<-了
		// 这里如果worker还需要继续执行任务 就需要判断workCount了
		// delete(c.doneChans, workerIdx) // 不需要删除啊 为什么要删除
		fmt.Printf("DoneCommit Normally: worerIdx[%d]\n", workerIdx)
		return // 因为将worker放入idle队列的任务DoneCommit已经做过了
	}

}

func DiliveryTask(c *Coordinator) {
	for {
		select {
		case worker := <-c.doneWorkerChan:
			// 有完成工作的worker进入 判断是否有任务即可
			// 只需要判断有没有执行异常的任务即可 因为不管是一开始加入就是idleWorker还是完成工作的worker
			// 先去找c.tasks 然后去找c.exceTask
			hasTask := false
			var args IssueByCoordinatorArgs
			var reply IssueByCoordinatorReply
			c.mutex.Lock()
			if len(c.tasks) != 0 {
				hasTask = true
				task := c.tasks[0]
				c.tasks = c.tasks[1:]
				worker.rule = Mapper
				worker.target = task
				args = IssueByCoordinatorArgs{
					Close:     false,
					Rule:      Mapper,
					TargetPos: task,
				}
			} else if len(c.exceTask) != 0 {
				hasTask = true
				execTask := c.exceTask[0]
				c.exceTask = c.exceTask[1:]
				worker.rule = execTask.rule
				worker.reduceIdx = execTask.reduceIdx
				worker.target = execTask.target
				args = IssueByCoordinatorArgs{
					Close:      false,
					Rule:       execTask.rule,
					ReducerIdx: execTask.reduceIdx,
					TargetPos:  execTask.target,
				}
			}
			c.mutex.Unlock()
			// 发送rpc请求
			if hasTask {
				socket, err := rpc.DialHTTP("unix", workerSock(worker.workerIdx))
				if err != nil {
					fmt.Printf("DialHttp error: workerIdx[%d]\n", worker.workerIdx)
				}
				err = socket.Call("Workerr.IssueByCoordinator", &args, &reply)
				if err != nil {
					fmt.Printf("Call error: workerIdx[%d]\n", worker.workerIdx)
				}
				OutWorkInfo(*worker)
				// TODO: 可能reducer也需要做
				if worker.rule == Mapper {
					go communication(worker.workerIdx, c)
				}
			} else {
				// 加入空闲队列
				worker.rule = Idle
				c.mutex.Lock()
				delete(c.workers, worker.workerIdx)
				c.idleWorker = append(c.idleWorker, worker)
				c.mutex.Unlock()
			}
			c.mutex.Lock()
			if c.ReducedOkCount == MaxReducerNum && len(c.workers) == 0 && len(c.idleWorker) == c.curWorkerCount {
				c.mutex.Unlock()
				return
			}
			c.mutex.Unlock()
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	ret := MaxReducerNum == c.ReducedOkCount && len(c.workers) == 0 && len(c.idleWorker) == c.curWorkerCount
	c.mutex.Unlock()
	if ret != true {
		return false
	}
	fmt.Println("####################")
	// 通知所有worker退出
	for _, workerInfo := range c.idleWorker {
		// 连接socket rpc调用
		sockName := workerSock(workerInfo.workerIdx)
		c, err := rpc.DialHTTP("unix", sockName)
		if err != nil {
			fmt.Printf("Dial To Worker Failed: workerIdx[%d]\n", workerInfo.workerIdx)
		}
		args := IssueByCoordinatorArgs{
			Close: true,
		}
		reply := IssueByCoordinatorReply{}
		err = c.Call("Workerr.IssueByCoordinator", &args, &reply)
		if err != nil {
			fmt.Printf("Send Close Failed: workerIdx[%d]\n", workerInfo.workerIdx)
		}
	}
	return ret
}

func OutWorkInfo(w WorkerInfo) {
	if w.rule == Mapper {
		fmt.Printf("WrokerInfo: Mapper workerId[%d] task[%s]\n", w.workerIdx, w.target)
	} else if w.rule == Reducer {
		fmt.Printf("WorkerInfo: Reducer workerId[%d] reduceIdx[%d]\n", w.workerIdx, w.reduceIdx)
	} else if w.rule == Idle {
		fmt.Printf("WorkerInfo: Idle workerId[%d]\n", w.workerIdx)
	}
}
