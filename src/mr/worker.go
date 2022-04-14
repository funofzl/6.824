/*
 * @Author: lxk
 * @Date: 2022-02-22 21:50:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-02-26 14:05:07
 */
package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// worker有两种角色
type RuleType int

const (
	Idle    RuleType = 0
	Mapper  RuleType = 1
	Reducer RuleType = 2
)

// 需要一个调用Call对象
type Workerr struct {
	IsDone     bool     // 是否已经完成 用于coordinator的返回
	Rule       RuleType // Map 还是reduce
	workerIdx  int      // 所有的worker在一起(决定连接的scoket)
	ReducerIdx int      // Reducer 应该处理哪些文件
	ReduceNum  int      // Reducer数量
	TargetPos  string   // task的目标文件位置

	ReduceBeginChan chan []int  // coordinator告诉reduce开始工作(哪个mapper)
	workChan        chan string // 接受下一步的指令(worker listen后进行设置 主协程开始工作)

	f *os.File // 用于存储reduce的结果
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// 1. 创建一个worker
	w := &Workerr{
		IsDone:          false,
		ReduceBeginChan: make(chan []int, 1),
		workChan:        make(chan string, 1),
	}

	// 2. 请求 Task
	CallTaskReq(w)
	w.server()

	// 因为可能多次执行 所以写在循环里
	for {
		if w.Rule == Mapper {
			// 如果是mapper 还需要请求reduceNum
			MaxReduceNum := CallReduceNumReq()

			// 3. Create file
			var err error
			var mappedFiles []*os.File
			for i := 0; i < MaxReduceNum; i++ {
				filename := fmt.Sprintf("mr-%d-%d", w.workerIdx, i)
				if _, err := os.Stat(filename); err != nil {
					if os.IsNotExist(err) {
						_, err = os.Create(filename)
						if err != nil {
							fmt.Println("File Created Failed!")
						}
					}
				}
				f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
				if err != nil {
					fmt.Printf("File Create Failed: %s\n", filename)
				}
				s, _ := f.Stat()
				fmt.Printf("%s -- %d", filename, s.Size())
				// for f := range mappedFiles {
				// if f.
				// }
				mappedFiles = append(mappedFiles, f)
			}
			// 4. Work
			filename := w.TargetPos
			file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			for _, kv := range kva {
				hash_value := ihash(kv.Key)
				reduceIdx := hash_value % MaxReducerNum
				// 先存起来 一起写入算了
				enc := json.NewEncoder(mappedFiles[reduceIdx])
				enc.Encode(&kv)
			}

			fmt.Printf("Write Succeed: worker[%d]\n", w.workerIdx)
			// 5. 通知coordinator
			CallDoneCommit(w)
		} else if w.Rule == Reducer {
			outFile := fmt.Sprintf("mr-out-%d", w.ReducerIdx)
			var err error
			if _, err = os.Stat(outFile); err != nil {
				if os.IsNotExist(err) {
					_, err = os.Create(outFile)
					if err != nil {
						fmt.Printf("Reduce Open Output File Failed: %s\n", outFile)
						return
					}
				} else {
					fmt.Println("Reduce File Stat Err")
					return
				}
			}
			w.f, err = os.OpenFile(outFile, os.O_WRONLY, 0666)
			if err != nil {
				fmt.Printf("Reduce Open Output File Failed: %s\n", outFile)
				return
			}
			// 文件打开失败怎么办

			// Work
			// 这里只能等待一个Mapper完成之后再进行工作(worker->coordinator->worker)
			// 所有mapper完成后 需要coordinator调用开始函数 使reducer知道开始work
			// 这里可以写一个for 循环 + select 自己定制延时行为(以免太长时间没有相应)
			mapperIdxArr := <-w.ReduceBeginChan
			// 开始读取mapperIdx生成的文件
			intermediate := []KeyValue{}
			for _, mapperIdx := range mapperIdxArr {
				mappedFile := fmt.Sprintf("mr-%d-%d", mapperIdx, w.workerIdx)
				fmt.Println(mappedFile)
				f, err := os.OpenFile(mappedFile, os.O_RDONLY, 0666)
				if err != nil {
					fmt.Printf("File Open Failed: file[%s]\n", mappedFile)
				}
				stat, _ := f.Stat()
				fmt.Println(stat.Size())
				// 从content中读取kv
				dec := json.NewDecoder(f)
				var kv KeyValue
				for {
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				fmt.Println(len(intermediate))
			}
			// 整合好数据 Reduce 开始
			sort.Sort(ByKey(intermediate))
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(w.f, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			s, _ := w.f.Stat()
			fmt.Println("-------", s.Size())
			w.f.Close()
			// -------------------------
			fmt.Printf("Write Succeed: worker[%d]", w.workerIdx)
			// 5. 通知coordinator
			CallDoneCommit(w)
		}

		// 6.等待coordinator的下一步命令
		// 在socket listen中 决定下一步的动作
		nextInstruction := <-w.workChan
		if nextInstruction == "Close" {
			fmt.Println("Finished!!!!!!")
			return
		} else if nextInstruction == "Work" {
			continue
		}
	}
	// 通信测试 uncomment to send the Example RPC to the coordinator.
	// fmt.Println("communication test")
	// CallExample()

}

func (w *Workerr) ReduceBegin(args *ReduceBeginArgs, reply *ReduceBeginReply) error {
	w.ReduceBeginChan <- args.MapperIdx
	fmt.Printf("Reduce Begin: mapperIdx[%d]\n", args.MapperIdx)
	return nil
}

func (w *Workerr) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := workerSock(w.workerIdx)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error: worker[%d] error[%s]", w.workerIdx, e)
	}
	fmt.Println("Listening...")
	go http.Serve(l, nil)
}

// 每隔10 coordinator就需要请求一次worker的Done方法
// 如果没有完成工作 返回false  就需要重新分配给别的worker(因为时间太长意味着可能失败了)
func (w *Workerr) Done(Args *WorkIsDoneArgs, Reply *WorkIsDoneReply) error {
	Reply.IsDone = w.IsDone
	Reply.Rule = w.Rule
	return nil
}

func (w *Workerr) IssueByCoordinator(args *IssueByCoordinatorArgs, reply *IssueByCoordinatorReply) error {
	// 这里只有两种选择 要么分配任务 要么close
	if args.Close {
		// 该关闭了
		w.workChan <- "Close"
	} else {
		w.Rule = args.Rule
		if w.Rule == Mapper {
			w.TargetPos = args.TargetPos
		} else if w.Rule == Reducer {
			w.ReducerIdx = args.ReducerIdx
		} else {
			fmt.Println("IssueByCoordinator Failed: Unknown RuleType ", w.Rule)
		}
		fmt.Println("instruction get")
		w.workChan <- "Work"
	}
	return nil
}

// 定义所有类型的Call 方法 比如请求任务 回复结果 ...
func CallTaskReq(w *Workerr) {
	// 请求的任务存储在 worker内
	args := TaskReqArgs{}
	reply := TaskReqReply{}

	ok := call("Coordinator.TaskReq", &args, &reply)
	if ok {
		// 判断Rule 并根据Rule类型决定接下来的动作
		w.Rule = reply.Rule
		w.workerIdx = reply.WorkerIdx
		if reply.Rule == Mapper {
			w.TargetPos = reply.TargetPos
			w.ReduceNum = reply.ReduceNum
		} else {
			w.ReducerIdx = reply.ReducerIdx
		}
		fmt.Printf("TaskReq Succeed!: worker[%d]\n", w.workerIdx)
	} else {
		fmt.Println("TaskReq Failed!")
	}
}

func CallReduceNumReq() int {
	args := ReduceNumReqArgs{}
	reply := ReduceNumReqReply{}
	ok := call("Coordinator.ReduceNumReq", &args, &reply)
	if ok {
		fmt.Printf("Call ReduceNum Succeed: reduceNum:[%d]\n", reply.ReduceNum)
		return reply.ReduceNum
	}
	fmt.Println("Call ReduceNum Failed!")
	return -1
}

func CallDoneCommit(w *Workerr) {
	args := DoneCommitArgs{
		Done:      true,
		Rule:      w.Rule,
		WorkerIdx: w.workerIdx,
	}
	reply := DoneCommitReply{}
	ok := call("Coordinator.DoneCommit", &args, &reply)
	if !ok {
		fmt.Printf("Call DoneCommit Failed: worker[%d]\n", w.workerIdx)
	} else {
		fmt.Printf("Call DoneCommit Succeed: worker[%d]\n", w.workerIdx)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
// 下面是rpc和coordinator进行交互 不需要修改
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234") 可以IP固定测试
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
