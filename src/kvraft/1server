package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const opTimeout = 500 * time.Millisecond

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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db                   map[string]string          // key/value pairs
	dispatcher           map[Notification]chan bool // 用于通知执行Start的协程可以返回client了
	lastAppliedRequestId map[int64]int64            // last requestId for every client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// ----------------------------------------------------//
	// 下面的判断是为了防止leader失败 切换新leader后再次执行
	// 只要log被commit 那就
	notify := Notification{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	exists, ch := kv.getChannel(notify)
	// ok 代表存在 直接接收即可
	// 即使正在apply 还没有创建 也不碍事 这里创建也一样 但是这里构造的index就不能apply了
	if !exists {
		command := Op{
			Name:      "Get",
			Key:       args.Key,
			ClientId:  args.ClientId,
			RequestId: args.RequestId,
		}
		_, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
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

	// 等待receiveApplier通知
	select {
	case <-ch:
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
		// TODO: isLeader的判断是否需要 什么情况下才会isDuplicate
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
	delete(kv.dispatcher, notify)
	kv.mu.Unlock()
	DPrintf("[Server-%d] [Get] (%s) %s", kv.me, args.Key, reply.Err)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// ----------------------------------------------------//
	notify := Notification{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	exists, ch := kv.getChannel(notify)
	if !exists {
		command := Op{
			Name:      args.Op,
			Key:       args.Key,
			Val:       args.Value,
			ClientId:  args.ClientId,
			RequestId: args.RequestId,
		}
		_, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
	}
	// ====================================================== //

	// 等待receiveApplier通知
	select {
	case <-ch:
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
	delete(kv.dispatcher, notify)
	kv.mu.Unlock()
	DPrintf("[Server-%d] [%s] (%s => %s) %s", kv.me, args.Op, args.Key, args.Value, reply.Err)
}

func (kv *KVServer) getChannel(notify Notification) (bool, chan bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.dispatcher[notify]
	if !ok {
		kv.dispatcher[notify] = make(chan bool, 1)
	}
	return ok, kv.dispatcher[notify]
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
	kv.dispatcher = make(map[Notification]chan bool)
	kv.lastAppliedRequestId = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// receive pplyMsg from raft
	go kv.receiveAndApply()

	return kv
}

func (kv *KVServer) receiveAndApply() {
	// 不需要看是否killed 因为applyChan没有就自己停了
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid == false {
			continue
		}
		op := applyMsg.Command.(Op)
		kv.mu.Lock()
		// 1. 首先判断是否是过去的
		curReuqestId, ok := kv.lastAppliedRequestId[op.ClientId]
		if !ok || op.RequestId > curReuqestId {
			// 2. apply to state machine (kv.db)
			switch op.Name {
			case "Put":
				kv.db[op.Key] = op.Val
			case "Append":
				kv.db[op.Key] += op.Val
			}
			// 3. modify the lastAppliedIndex for this client
			kv.lastAppliedRequestId[op.ClientId] = op.RequestId
		}
		notify := Notification{
			ClientId:  op.ClientId,
			RequestId: op.RequestId,
		}
		ch, ok := kv.dispatcher[notify]
		if !ok {
			kv.dispatcher[notify] = make(chan bool, 1)
		}
		kv.mu.Unlock()
		// 4. notify the Start goroutine.(follower not need to do this)
		DPrintf("[Server-%d] Cid-Rid:[%d-%d] [%s] (%s => %s) applied", kv.me, op.ClientId, op.RequestId, op.Name, op.Key, op.Val)
		ch <- true
	}
}
