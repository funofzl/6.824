package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const opTimeout = 1000 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name      string // 三种类型 GET PUT APPEND
	Key       string
	Val       string
	ClientId  int64
	RequestId int64
}

type Notification struct {
	ClientId  int64
	RequestId int64
	Val       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db                   map[string]string         // key/value pairs
	dispatcher           map[int]chan Notification // 用于通知执行Start的协程可以返回client了
	lastAppliedRequestId map[int64]int64           // last requestId for every client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// ----------------------------------------------------//
	// 下面的判断是为了防止leader失败 切换新leader后再次执行
	// 只要log被commit 那就
	// ok 代表存在 直接接收即可
	// 即使正在apply 还没有创建 也不碍事 这里创建也一样 但是这里构造的index就不能apply了
	command := Op{
		Name:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	/* *********************************************** //
		不能都从applyChan读取 因为程序指令执行不确定性
		可能先获得index的先阻塞先拿到applyMsg
		|  Start(1)       |             |
		|            	  |  Start(2)   |
		|    			  | <-applyChan | 这个会得到1
		|  <-applyChan    | 			| 这个会得到2
		所以涉及到channel的东西尽可能一个协程来就好了
	// *********************************************** */
	// ====================================================== //
	ch := kv.getChannel(index)
	// 等待receiveApplier通知
	select {
	case notidy := <-ch:
		if notidy.ClientId != args.ClientId || notidy.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
			return
		}
		// get value from DB
		kv.mu.Lock()
		value, ok := kv.db[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	case <-time.After(opTimeout):
		DPrintf("[Server-%d] [Get] (%s) timeout", kv.me, args.Key)
		// _, isLeader := kv.rf.GetState()
		// if isLeader && kv.isDuplicate(args.ClientId, args.RequestId) {
		if kv.isDuplicate(args.ClientId, args.RequestId) {
			kv.mu.Lock()
			value, ok := kv.db[args.Key]
			kv.mu.Unlock()
			if ok {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Value = ""
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Lock()
	delete(kv.dispatcher, index)
	kv.mu.Unlock()
	DPrintf("[Server-%d] [Get] (%s) %s", kv.me, args.Key, reply.Err)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// ----------------------------------------------------//
	command := Op{
		Name:      args.Op,
		Key:       args.Key,
		Val:       args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// ====================================================== //

	ch := kv.getChannel(index)
	// 等待receiveApplier通知
	select {
	case notidy := <-ch:
		if notidy.ClientId != args.ClientId || notidy.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
	case <-time.After(opTimeout):
		// 判断是否是过去的
		DPrintf("[Server-%d] [%s] (%s => %s) timeout", kv.me, args.Op, args.Key, args.Value)
		// _, isLeader := kv.rf.GetState()
		// if isLeader && kv.isDuplicate(args.ClientId, args.RequestId) {
		if kv.isDuplicate(args.ClientId, args.RequestId) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Lock()
	delete(kv.dispatcher, index)
	kv.mu.Unlock()
	DPrintf("[Server-%d] [%s] (%s => %s) %s", kv.me, args.Op, args.Key, args.Value, reply.Err)
}

func (kv *KVServer) getChannel(index int) chan Notification {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.dispatcher[index]
	if !ok {
		kv.dispatcher[index] = make(chan Notification, 1)
	}
	return kv.dispatcher[index]
}

func (kv *KVServer) isDuplicate(clientId int64, RequestId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	curRequestId, ok := kv.lastAppliedRequestId[clientId]
	if ok && curRequestId >= RequestId {
		return true
	}
	return false
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.dispatcher = make(map[int]chan Notification)
	kv.lastAppliedRequestId = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 尝试读取快照
	kv.InstallSnapShot(persister.ReadSnapshot())
	// You may need initialization code here.
	// receive applyMsg from raft
	go kv.receiveAndApply()

	return kv
}

// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int

// 	// For 2D:
// 	SnapshotValid bool
// 	Snapshot      []byte
// 	SnapshotTerm  int
// 	SnapshotIndex int
// }

func (kv *KVServer) receiveAndApply() {
	// 不需要看是否killed 因为applyChan没有就自己停了
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			kv.ApplyCommand(applyMsg)
		} else if applyMsg.SnapshotValid {
			kv.ApplySnapShot(applyMsg)
		}
	}
}

func (kv *KVServer) ApplyCommand(applyMsg raft.ApplyMsg) {
	op, ok := applyMsg.Command.(Op)
	if !ok {
		return
	}
	kv.mu.Lock()
	// 1. <<<<<<<< 首先判断是否是过去的 >>>>>>>>
	curReuqestId, ok := kv.lastAppliedRequestId[op.ClientId]
	if !ok || op.RequestId > curReuqestId {
		// <<<<<<<< 2. apply to state machine (kv.db) >>>>>>>
		switch op.Name {
		case "Put":
			kv.db[op.Key] = op.Val
		case "Append":
			kv.db[op.Key] += op.Val
		}
		// <<<<<<<< 3. modify the lastAppliedIndex for this client >>>>>>>
		kv.lastAppliedRequestId[op.ClientId] = op.RequestId
	}
	notify := Notification{
		ClientId:  op.ClientId,
		RequestId: op.RequestId,
	}
	ch, ok := kv.dispatcher[applyMsg.CommandIndex]
	// 4. <<<<<<<< judge if need to snapshot >>>>>>>
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		kv.TrySnapshot(applyMsg.CommandIndex)
	}
	kv.mu.Unlock()
	// <<<<<<<<   5. notify the Start goroutine.(follower not need to do this) >>>>>>>
	DPrintf("[Server-%d] Cid-Rid:[%d-%d] [%s] (%s => %s) applied", kv.me, op.ClientId, op.RequestId, op.Name, op.Key, op.Val)
	if ok {
		ch <- notify
	}
	// -------------------------------------------------------------//
	// 对于新的Leader client发起的重复请求会超时
	// 这时会发现已经请求已经旧了旧直接返回(Put&Append)或者读取(Get)
	// 对于单个Client的读取来说，即使是并发 其实也没有什么严格的顺序的 因为网络和指令执行原因
	// 所以假如切换新的leader之后，client后发起的请求正常返回，上一个重复请求因为延时后返回也没什么大问题
	// -------------------------------------------------------------//
}

func (kv *KVServer) ApplySnapShot(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
		kv.InstallSnapShot(applyMsg.Snapshot)
	}
}

// 安装快照
// 1. Start初始化时
// 2. Raft的InstallSnapShot时
func (kv *KVServer) InstallSnapShot(snapshot []byte) {
	// 上层函数ApplySnapshot已加锁 | Start不需要
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	b := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(b)
	var db map[string]string
	var lastAppliedRequestId map[int64]int64
	if d.Decode(&lastAppliedRequestId) != nil || d.Decode(&db) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!", kv.me)
	} else {
		kv.lastAppliedRequestId = lastAppliedRequestId
		kv.db = db
	}
}

// 生成快照
// 每次接收applyMsg时都监测一下是否可以生成快照
func (kv *KVServer) TrySnapshot(lastApplyIndex int) {
	// snapshot the raft state
	// 上层函数ApplyCommand已加锁
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	// encode the state(DB) and lastRequestId of all clients
	e.Encode(kv.lastAppliedRequestId)
	e.Encode(kv.db)
	// 把已经apply的状态都持久化
	kv.rf.Snapshot(lastApplyIndex, b.Bytes())
}
