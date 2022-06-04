/*
 * @Author: lxk
 * @Date: 2022-02-22 21:50:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-06-04 14:30:37
 */
package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"time"
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

const waitTime = 1 * time.Second

// 需要一个调用Call对象
type Workerr struct {
	workerId string // 一个唯一Id 用于标记worker
	TaskInfo        // task 信息
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
	// -----------------    1. 创建一个worker ---------------------
	w := &Workerr{}

	// 随机一个WorkerIdx
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	n := 4
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	w.workerId = string(b)
	// 开启监听
	w.server()
	for {
		// --------------------   2. 请求 Task   ----------------------
		CallTaskReq(w)
		switch w.Role {
		case Mapper:
			// -----------   3. Create files (intermediate files) ---------
			var err error
			var mappedFiles []*os.File
			for i := 0; i < w.ReduceNum; i++ {
				filename := fmt.Sprintf("mr-%d-%d", w.PartIndex, i)
				if _, err := os.Stat(filename); err != nil {
					if os.IsNotExist(err) {
						_, err = os.Create(filename)
						if err != nil {
							fmt.Println("File Created Failed!")
						}
					}
				}
				f, err := os.OpenFile(filename, os.O_WRONLY, 0666)
				if err != nil {
					fmt.Printf("File Create Failed: %s\n", filename)
				}
				mappedFiles = append(mappedFiles, f)
			}
			// ------------------  4. Read input file  -------------------
			filename := w.FileName
			file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			// -------------  5. Work and Write to intermediate file ----------
			kva := mapf(filename, string(content))

			for _, kv := range kva {
				hash_value := ihash(kv.Key)
				reduceIdx := hash_value % w.ReduceNum
				// 先存起来 一起写入算了
				enc := json.NewEncoder(mappedFiles[reduceIdx])
				enc.Encode(&kv)
			}
			// fmt.Printf("Write Succeed: worker[%s]\n", w.workerId)

			// ------------------- 6. 通知coordinator -----------------
			CallDoneCommit(w)

		case Reducer:
			// -------------------  3. Open output file --------------
			outFile := fmt.Sprintf("mr-out-%d", w.PartIndex)
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
			file, err := os.OpenFile(outFile, os.O_WRONLY, 0666)
			if err != nil {
				fmt.Printf("Reduce Open Output File Failed: %s\n", outFile)
				return
			}

			// ---------------- 4. Read intermediate files  --------------------
			intermediate := []KeyValue{}
			for mapperIdx := 0; mapperIdx < w.MapNum; mapperIdx++ {
				mappedFile := fmt.Sprintf("mr-%d-%d", mapperIdx, w.PartIndex)
				// fmt.Println(mappedFile)
				f, err := os.OpenFile(mappedFile, os.O_RDONLY, 0666)
				if err != nil {
					fmt.Printf("File Open Failed: file[%s]\n", mappedFile)
				}
				// 从content中读取kv
				dec := json.NewDecoder(f)
				var kv KeyValue
				for {
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				// fmt.Println(len(intermediate))
			}
			// ---------------  5. Sort and Reduce --------------------
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
				fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			// s, _ := file.Stat()
			// fmt.Println("-------", s.Size())
			file.Close()
			// -------------------------
			// fmt.Printf("Write Succeed: worker[%s]", w.workerId)
			// ------------------   6. 通知coordinator   ------------------
			CallDoneCommit(w)
		case Idle:
			time.Sleep(waitTime)
		case Done:
			return // 退出
		}
	}
	// 通信测试 uncomment to send the Example RPC to the coordinator.
	// fmt.Println("communication test")
	// CallExample()

}

func (w *Workerr) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := workerSock(w.workerId)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error: worker[%d] error[%s]", w.PartIndex, e)
	}
	fmt.Println("Listening...")
	go http.Serve(l, nil)
}

func CallTaskReq(w *Workerr) {
	// 请求的任务存储在 worker内
	args := TaskReqArgs{}
	reply := TaskReqReply{}

	ok := call("Coordinator.TaskReq", &args, &reply)
	if ok {
		// 判断Role 并根据Role类型决定接下来的动作
		w.Role = reply.Role
		w.PartIndex = reply.PartIndex
		if reply.Role == Mapper {
			w.FileName = reply.FileName
			w.ReduceNum = reply.ReduceNum
		} else {
			w.MapNum = reply.MapNum
		}
		// fmt.Printf("TaskReq Succeed!: worker[%s]\n", w.workerId)
	} else {
		// fmt.Println("TaskReq Failed!")
	}
}

func CallDoneCommit(w *Workerr) {
	args := DoneCommitArgs{
		Done:      true,
		WorkerId:  w.workerId,
		Role:      w.Role,
		PartIndex: w.PartIndex,
	}
	reply := DoneCommitReply{}
	ok := call("Coordinator.DoneCommit", &args, &reply)
	if !ok {
		// fmt.Printf("Call DoneCommit Failed: worker[%s]\n", w.workerId)
	} else {
		// fmt.Printf("Call DoneCommit Succeed: worker[%s]\n", w.workerId)
	}
}

// ----------------------- Don't Touch the following!!! ------------------------------------
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
