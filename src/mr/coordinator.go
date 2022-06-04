/*
 * @Author: lxk
 * @Date: 2022-02-22 21:50:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-06-04 15:01:07
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

const maxExecTime = 10 * time.Second
const MapNum = 2
const MaxReducerNum = 2

type TaskStat struct {
	startTime time.Time // 每个运行中的任务都有开始时间
	workerId  string    // worker for task
	TaskInfo            // 任务具体信息
}

type Coordinator struct {
	// Your definitions here.
	mutex     sync.Mutex         //用于更新下面的
	reduceNum int                // 最后生成的文件个数
	mapNum    int                // 中间文件编号
	taskQueue []TaskStat         // 所有待开始的任务
	running   map[int][]TaskStat // 正在运行的任务(需要立马找到所以要有key，可能有多个worker在执行该任务)

	finished bool // 是否已完成 TaskReq时需要返回
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Coordinator需要初始化
	c := Coordinator{
		reduceNum: nReduce,
		mapNum:    len(files),
		taskQueue: []TaskStat{},
		running:   make(map[int][]TaskStat),
		finished:  false,
	}
	for i, file := range files {
		task := TaskStat{
			TaskInfo: TaskInfo{ // TODO: 注意此处嵌结构的用法
				Role:      Mapper,
				PartIndex: i,
				FileName:  file,
				ReduceNum: c.reduceNum,
			},
		}
		c.taskQueue = append(c.taskQueue, task)
	}
	fmt.Printf("MakeCoordinator Succeed: there are [%d] tasks\n", len(c.taskQueue))
	// Your code here.
	// 启动监听服务
	c.server()
	// 监控每一个任务的完成时间
	go Tick(&c)
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
	defer c.mutex.Unlock()
	// 如果需要退出
	if c.finished {
		reply.Role = Done
		return nil
	}
	// ---------   如果没有任务
	if len(c.taskQueue) == 0 {
		reply.Role = Idle
		return nil
	}
	// ----------  如果有任务
	// 1. 放入running 队列
	taskStat := c.taskQueue[0]
	c.taskQueue = c.taskQueue[1:]
	taskStat.startTime = time.Now()
	taskStat.workerId = args.WorkerId
	// 下面不用判断key是否存在 直接append 默认为类型0值
	c.running[taskStat.PartIndex] = append(c.running[taskStat.PartIndex], taskStat)
	// 2. reply
	reply.TaskInfo = taskStat.TaskInfo
	return nil
}

func (c *Coordinator) DoneCommit(args *DoneCommitArgs, reply *DoneCommitReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// -----------   1. delete entry from c.taskQueue.   -------------
	// Note that perhaps multiple workers work for one task
	if _, ok := c.running[args.PartIndex]; ok {
		// 在Reduce最后阶段多个worker执行一个任务的时候，第一个先完成的删除，后面就直接发送停止信号
		delete(c.running, args.PartIndex)
	} else {
		return nil // 直接返回 因为这个完成没有意义了
	}
	if len(c.taskQueue) == 0 && len(c.running) == 0 {
		switch args.Role {
		case Mapper:
			// ----------- 2. 如果是最后一个Mapper，就需要初始化Reducer任务 -------
			for i := 0; i < c.reduceNum; i++ {
				taskStat := TaskStat{
					TaskInfo: TaskInfo{
						Role:      Reducer,
						PartIndex: i,
						MapNum:    c.mapNum,
					},
				}
				c.taskQueue = append(c.taskQueue, taskStat)
			}
		case Reducer:
			// 2. 所有工作已经做完 结束任务
			fmt.Println("All tasks have been done!")
			c.finished = true
			// for _, workerStat := range c.idlingQueue {
			// 	c, err := rpc.DialHTTP("unix", workerSock(workerStat.workerId))
			// 	if err != nil {
			// 		fmt.Printf("End DialHttp error: worker[%d]\n", workerStat.workerId)
			// 	}
			// 	err = c.Call("Workerr.End", &args, &reply)
			// 	if err != nil {
			// 		fmt.Printf("End Call error: worker[%d]\n", workerStat.workerId)
			// 	}
			// }
		}
	}
	return nil
}

func Tick(c *Coordinator) {
	for {
		time.Sleep(1 * time.Second)
		c.mutex.Lock()
		if c.finished {
			c.mutex.Unlock()
			return
		}
		now := time.Now()
		for _, tasks := range c.running {
			for _, task := range tasks {
				if now.After(task.startTime.Add(maxExecTime)) {
					c.taskQueue = append(c.taskQueue, task)
				}
			}
		}
		c.mutex.Unlock()
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := c.finished
	return ret
}

func OutWorkInfo(workerId string, w TaskStat) {
	if w.Role == Mapper {
		fmt.Printf("WrokerInfo: Mapper workerId[%s] File[%s] PartIndex[%d]\n", workerId, w.FileName, w.PartIndex)
	} else if w.Role == Reducer {
		fmt.Printf("WorkerInfo: Reducer workerId[%s] PartIndex[%d]\n", workerId, w.PartIndex)
	}
}
