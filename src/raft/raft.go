package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Rule int

const (
	Follower  Rule = 0
	Candidate Rule = 1
	Leader    Rule = 2
)

const heartBeatInterval = 100 * time.Millisecond

type LogEntry struct {
	Term    int
	Index   int // 不使用位置来表示index
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// need to persistent
	rule     Rule
	curTerm  int
	votedFor int

	// volatile
	commitIndex int // append的log
	lastApplied int // 实际提交的

	// for leader 选举完成后需要记录每个server的matchIndex和nextIndex
	// 其实这两个是不是冗余了
	nextIndex  []int // serverIdx => nextIndex
	matchIndex []int // serverIdx => nextIndex

	log []LogEntry // Log

	heartsbeats chan bool // 别人给自己发送heartbeat
	// lastHeartbeatTime int64     // 防止断联之后大term选举
	leaderChan chan bool // 自己成为leader之后要立马通知还在slect timeout 的ticker
	appendChan chan bool // leader发送appendEnties之后就不需要再发送heartbest了

	applyChan chan ApplyMsg
	applyCond *sync.Cond // 唤醒apply协程
}

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func ArrMin(a []int) int {
	min := a[0]
	for i := range a {
		if a[i] < min {
			min = a[i]
		}
	}
	return min
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.curTerm
	isleader = rf.rule == Leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) getState() []byte {
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// // 用于传递给kvServer 进行snapShot 最多可以到commitIndex处
// func (rf *Raft) GetIndexAndSnapshot() (int, []byte) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	e.Encode(rf.curTerm)
// 	e.Encode(rf.votedFor)
// 	baseIndex := rf.GetFirstIndex()
// 	e.Encode(rf.log[:rf.commitIndex-baseIndex+1])
// 	return rf.commitIndex, w.Bytes()
// }

// KVServer调用判断是否需要snapshot
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.getState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&curTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("Decode error!!!!")
	} else {
		rf.curTerm = curTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.GetFirstIndex() || index > rf.commitIndex {
		// 旧的Snapshot 或者 还没有提交的
		return
	}
	// 删除前边的log (要考虑到GC)
	old_snaphost_index := rf.GetFirstIndex()
	// 这里还是留下了index所对应的log作为第一个 不然需要添加SnopShotIndex等状态 而且replicate log会修改很多东西
	rf.log = append([]LogEntry(nil), rf.log[index-rf.GetFirstIndex():]...)
	rf.log[0].Command = nil // 第一个logEntry只是描述lastIndex和lastTerm用的
	rf.persister.SaveStateAndSnapshot(rf.getState(), snapshot)
	DPrintf("after snapshoting {Node %v}'s state is { commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.commitIndex, rf.lastApplied, rf.log[0], rf.log[len(rf.log)-1], index, old_snaphost_index)
}

//#######################################################
//#   RequestVote
//######################################################
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int // 表明身份
	Term         int // 表明自己的任期
	LastLogIndex int // 日志最新任期  用于判断是否要投票给他
	LastLogTerm  int // 最新日志位置  用于判断是否要投票给他
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // 如果curTerm > args.term 就要返回curTerm
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 有一个现象就是断联之后的节点恢复之后会以大term出发选举，但是有不满足up to date的log
	// if time.Now().UnixNano() < rf.lastHeartbeatTime+int64(heartBeatInterval) {
	// DPrintf("[%d] is normal, so can not vote for [%d]\n", rf.me, args.CandidateId)
	// return
	// }
	DPrintf("[%d] received the voteRequest from [%d]\n", rf.me, args.CandidateId)
	// 1. args.Term < rf.term
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm // 只有在没有选举成功的时候才需要返回term
		reply.VoteGranted = false
		DPrintf("[%d] can not vote for [%d](small term)\n", rf.me, args.CandidateId)
		return
	}
	// 不论能都投票成功 都要变到大的任期 任期不可能回退的 否则新旧之争可能不会结束
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.votedFor = -1
		rf.rule = Follower
		rf.persist()
	}
	//	 要想投票 必须验证up-to-date
	// 2. 判断up to date条件
	selfLastLogIndex := rf.GetLastIndex()
	selfLastLogTerm := -1
	if selfLastLogIndex > 0 {
		selfLastLogTerm = rf.log[len(rf.log)-1].Term
	}
	if (args.LastLogTerm < selfLastLogTerm) ||
		(args.LastLogTerm == selfLastLogTerm && args.LastLogIndex < selfLastLogIndex) {
		reply.VoteGranted = false
		reply.Term = rf.curTerm
		// 在这种情况下 早年失联的leader 会将term减小
		DPrintf("[RequestVote]:[%d] can not vote for: [%d] (not up to date)", rf.me, args.CandidateId)
		return
	}

	// 通过考验之后 两种情况可以投票(没有投票或者是本来就是他这种是为什么)
	// cotedFor == nil or CandidateId
	// 这里可以趁机更新votedFor(更新的任期 就需要重新投票了 毕竟时代变了)
	// 就是说如果arg.term更新 就需要重置为follower 并设置为可以投票状态
	// 这里意味着 follower和candidate都会在新的任期投给request(并更新到新的任期)
	// 但是如果是同样的term  就不会再投票了(即使request数据更新 谁让request下手慢了呢 总不能一个时代投两次票吧)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.rule = Follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true // 投票就不需要管term了 肯定和request一样了呀

		rf.heartsbeats <- true // 投票成功(1/3 reset heartbeat)
		rf.persist()
		DPrintf("[RequestVote]:[%d] vote for: [%d]", rf.me, args.CandidateId)
	}
}

//#######################################################
//#   AttemptElection
//#######################################################
func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	// if rf.rule != Candidate {
	// 	rf.mu.Unlock()
	// 	return
	// }
	rf.curTerm++
	DPrintf("[%d] begin election in term --- [%d] ---\n", rf.me, rf.curTerm)
	rf.votedFor = rf.me
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.curTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastIndex(),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	var vote int64 = 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.curTerm {
				DPrintf("[%d] received higher term [%d]so chenged to follower\n", rf.me, reply.Term)
				rf.curTerm = reply.Term
				rf.votedFor = -1
				rf.rule = Follower
				rf.persist()
				rf.heartsbeats <- true
				return
			}

			// 1. 判断不能是 Follower 因为可能在这个过程中可能已经有leader了
			if rf.rule == Follower || rf.rule == Leader {
				return // 已经产生leader了
			}
			// 2. 可能已经到新的任期了 才收到回复 这时候应该忽略(重要 看别人写的)
			if rf.curTerm > args.Term {
				return
			}
			// 2.没有收到票
			if !reply.VoteGranted {

				return
			}

			atomic.AddInt64(&vote, 1)
			// 判断是否可以成为leader
			DPrintf("[%d]'s vote is [%d]\n", rf.me, vote)
			if int(atomic.LoadInt64(&vote)) >= len(rf.peers)/2+1 {
				rf.rule = Leader
				// 成为leader之后要开始发送心跳包了 在ticker函数中实现
				rf.leaderChan <- true
			}
		}(server)
	}
}

//#######################################################
//#   AppendEntity RPC
//#######################################################

type AppendEntityArgs struct {
	Term         int // leader's term
	LeaderId     int // leader's Id
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // empty for heartbeat
	LeaderCommit int        // leader's commit index
}

/*
	term 应该是用于leader知道从哪里开始更新 不需要一步步退了
	NoAction 万一多的在前 少的岂不是会覆盖多的 所以判断是否append并通过NoAction返回
*/
type AppendEntityReply struct {
	Term      int // currentTerm, for leader to update itself
	Success   bool
	NextIndex int // 懒惰回退式 不然log体量大起来的时候就不行了
}

func (rf *Raft) AppendEntity(args *AppendEntityArgs, reply *AppendEntityReply) {
	// 这里被远程调用
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ----------- leader 发生异常 -------------------
	if args.Term < rf.curTerm {
		DPrintf("arg.term < rf.curterm")
		reply.Success = false
		reply.Term = rf.curTerm
		return
	}
	// -------------------------------------------------------------
	// 正常的AppendEntity都可以作为心跳
	// rf.lastHeartbeatTime = time.Now().UnixNano()
	rf.heartsbeats <- true // 收到AppendEntity(1/3 heartbeat)
	outputArgs("AppendEntity", rf.me, *args)
	//---------------------------------------------------------------
	// 可能集群出现问题(比如某个server失联后重新回到集群) 或者 正常的candidate转为follower
	DPrintf("server[%d] term:[%d] commit[%d]", rf.me, rf.curTerm, rf.commitIndex)
	if args.Term > rf.curTerm || rf.rule == Candidate {
		// 需要改变角色 收到了leader的通知
		rf.curTerm = args.Term
		rf.rule = Follower
		rf.votedFor = -1 //这里其实不重要 因为在requestVote中更高的term是可以重置votedFor的
		rf.persist()
	}
	reply.Term = args.Term // 反正这个时候curTerm已经和args.Term相等了

	// ###############################################################//
	// ################-------  log 逻辑 ----------###################//
	// ###############################################################//

	// 第一次leader发送空集和最大的lastLogIndex
	// 两种情况
	// 1. follower log太少了 告知位置 返回
	if args.PrevLogIndex > rf.GetLastIndex() {
		reply.NextIndex = rf.GetLastIndex() + 1 // 从哪个位置开始
		reply.Success = false
		return
	}
	// 2. 判断是否任期冲突
	// 2. 冲突 找到不冲突的位置 告知返回
	baseIndex := rf.GetFirstIndex() // 其实indx还是连续的 但是开头不是0 所以才这样
	// if args.PrevLogIndex < baseIndex {
	// 	DPrintf("[Error][AppendEntity] server:[%d]' baseIndex > leader[%d]'s index\n", rf.me, args.LeaderId)
	// 	return
	// }
	if args.PrevLogIndex >= baseIndex {
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		// 冲突的话就找到非冲突任期的位置(冲突任期的上一个任期)
		// DPrintf("term[%d] prevTerm[%d]", term, args.PrevLogTerm)
		if term != args.PrevLogTerm {
			// 找到上一个term的结尾位置 可能上一个term就好起来了
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.log[i-baseIndex].Term != term {
					reply.NextIndex = i + 1
					break
				}
			}
			reply.Success = false
			return
		}
	} else {
		DPrintf("AppendEntity args.PrevLogIndex < baseIndex")
		reply.Success = true
		return
	}

	reply.Success = true
	// 并发的时候进行判断
	if len(args.Entries) > 0 {
		argsfinalIndex := args.PrevLogIndex + len(args.Entries)
		if argsfinalIndex <= rf.GetLastIndex() &&
			args.Entries[len(args.Entries)-1].Term == rf.LogAt(argsfinalIndex).Term {
		} else {
			remainPos := args.PrevLogIndex - baseIndex + 1
			// ########################################################################//
			// ------------  1. 可以更新了(先截断 以防后面有数据) ------------------------
			// DPrintf("server[%d]'log[%v]", rf.me, rf.log)
			rf.log = rf.log[:remainPos]
			rf.log = append(rf.log, args.Entries...)
			rf.persist()
			reply.NextIndex = rf.GetLastIndex() + 1
		}
	}
	// ------------  2. 更新commitIndex和applyId -------------------------------
	if rf.commitIndex < args.LeaderCommit { // 有更新的余地
		// 更新为LastLogIndex 和 leaderCommit中的较小者
		DPrintf("server[%d] commitIndex[%d] new_min[%d]", rf.me, rf.commitIndex, Min(args.LeaderCommit, rf.GetLastIndex()))
		rf.commitIndex = Min(args.LeaderCommit, rf.GetLastIndex())
		rf.applyCond.Broadcast()
	}
}

//#######################################################
//#  follower InstallSnapshot RPC
//#######################################################
// 不要求分段发送 不需要offset和done
type InstallsnapshotArgs struct {
	Term              int // leader’s term
	LeaderId          int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of lastIncludedIndex
	// offset            int    // byte offset where chunk is positioned in the snapshot file
	Data []byte // raw bytes of the snapshot chunk, starting at offset
	// done              bool   // true if this is the last chunk
}

type InstallsnapshotReply struct {
	Term int // for leader to update itself
}

func (rf *Raft) Installsnapshot(args *InstallsnapshotArgs, reply *InstallsnapshotReply) {
	// 1. Reply immediately if term < currentTerm
	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot
	// 	  with a smaller index
	// 6. If existing log entry has same index and term as snapshot’s
	//    last included entry, retain log entries following it and reply
	// 7. Discard the entire log
	// 8. Reset state machine using snapshot contents (and load
	//    snapshot’s cluster configuration)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server[%d] is installing snapshot from leader", rf.me)
	// 1. 判断term
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}
	reply.Term = args.Term
	if args.Term > rf.curTerm { // 新的任期需要重置votedFor
		rf.curTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	//无条件认为对方是leader 自己转为follower
	if rf.rule != Follower {
		rf.rule = Follower
	}
	// 2. 更新heartbeat (和AppendEnttyRPC类似 需要更新heartbeat)
	rf.heartsbeats <- true

	// ###################################################################//
	// ---------------------------  snapshot逻辑 --------------------------------------
	// 和提交的比 不需要和apply比较 判断快照是否比较旧了
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	DPrintf("follower [%d] is installing snapshot okkkkkkk", rf.me)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() {
		rf.applyChan <- applyMsg
	}()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	// 判断是否install snapshot
	// 上层service接收到snapshot后再调用CondInstallSnapshot
	// 为什么不直接Install Snapshot然后通知service呢
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		// 赶上来了 不需要快照了
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}
	if lastIncludedIndex <= rf.GetLastIndex() &&
		lastIncludedTerm == rf.log[lastIncludedIndex-rf.log[0].Index].Term {
		rf.log = append([]LogEntry(nil), rf.log[lastIncludedIndex-rf.GetFirstIndex():]...)
		rf.log[0].Command = nil
	} else {
		rf.log = append([]LogEntry(nil), LogEntry{
			Term:    lastIncludedTerm,
			Index:   lastIncludedIndex,
			Command: nil,
		})
	}
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	// 1.Raft安装快照 保存状态
	rf.persister.SaveStateAndSnapshot(rf.getState(), snapshot)
	DPrintf("{Node %v}'s state is {commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.commitIndex, rf.lastApplied, rf.log[0], rf.log[len(rf.log)-1], lastIncludedTerm, lastIncludedIndex)
	return true
}

//#######################################################
//#  leader sendSnapshot
//#######################################################

func (rf *Raft) sendSnapshot(server int, args InstallsnapshotArgs) {
	// 到了这里 说明数据比较旧了 已经形成快照了 follower想要 那就给你快照算了
	reply := InstallsnapshotReply{}
	ok := rf.peers[server].Call("Raft.Installsnapshot", &args, &reply)
	if ok {
		DPrintf("[%d] send heartbeat to [%d] succeed", rf.me, server)
	} else {
		DPrintf("[%d] send heartbeat to [%d] failed", rf.me, server)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.curTerm {
		DPrintf("[Change] leader[%d] need to update(snapshot).....", rf.me)
		rf.curTerm = reply.Term
		rf.rule = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	// 更新完成
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	DPrintf("server[%d] after: nextIndex[%d] matchIndex[%d]", server, rf.nextIndex[server], rf.matchIndex[server])
	// 因为快照里的都是提交过的 索引不需要再提交了
}

//#######################################################
//#  broadCastAppendEnries
//#######################################################
func (rf *Raft) broadCastAppendEnries() {
	rf.mu.Lock()
	DPrintf("leader[%d]broadCastAppendEnries...................................", rf.me)
	args := AppendEntityArgs{
		Term:         rf.curTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: 0,   // 根据server定制化
		PrevLogTerm:  0,   // 根据server定制化
		Entries:      nil, // 根据server定制化
	}
	rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			// 1. 想要太旧的数据(已经在快照里了 不在leader's log里了)就需要发送快照了
			if rf.nextIndex[server] <= rf.GetFirstIndex() {
				snapArgs := InstallsnapshotArgs{
					Term:              rf.curTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.GetFirstIndex(),
					LastIncludedTerm:  rf.log[0].Term,
					Data:              rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()
				rf.sendSnapshot(server, snapArgs)
				return
			}
			// 2. heartbeat和 normal AppendEntity
			// DPrintf("[%d] send hearteat to [%d]\n", args.LeaderId, server)
			personalArgs := args
			personalArgs.PrevLogIndex = rf.matchIndex[server]
			realPos := rf.matchIndex[server] - rf.GetFirstIndex()
			personalArgs.PrevLogTerm = rf.log[realPos].Term
			personalArgs.Entries = rf.log[realPos+1:] // Next始终比match大1
			// outputArgs("heartbeat", server, personalArgs)
			rf.mu.Unlock()
			// 计算自己需要的起始位置
			reply := AppendEntityReply{}
			ok := rf.peers[server].Call("Raft.AppendEntity", &personalArgs, &reply)
			if ok {
				DPrintf("[%d] send heartbeat to [%d] succeed", args.LeaderId, server)
			} else {
				return
			}
			// 每次发完都要检查一下是否可以更新nextIndex和matchIndex 以及 commitIndex
			DPrintf("sssss##############################[%d]", server)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("sssss##############################[%d]", server)
			// 可能已经不是leader了 或者回复已经过时了
			if rf.rule != Leader || rf.curTerm != personalArgs.Term {
				DPrintf("rf.curTerm != personalArgs.Term result from server[%d]", server)
				return
			}
			// DPrintf("real result [%d] %v\n", server, reply)
			if reply.Success {
				DPrintf("matchIndex[%d]", rf.matchIndex[server])
				if len(personalArgs.Entries) == 0 {
					// 没什么需要发的就一个心跳包而已 不用更新什么
					return
				}
				// 增大nextIndex matchIndex 判断N那个 更新commitIndex
				DPrintf("server[%d] before: nextIndex[%d] matchIndex[%d]", server, rf.nextIndex[server], rf.matchIndex[server])
				rf.matchIndex[server] = Max(rf.matchIndex[server], personalArgs.Entries[len(personalArgs.Entries)-1].Index)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DPrintf("server[%d] after: nextIndex[%d] matchIndex[%d]", server, rf.nextIndex[server], rf.matchIndex[server])
				// 如何更新commitIndex 需要大多数的follower都有了才行
				// 注意只有当前的term 才能进行commit 所以判断一下
				if personalArgs.Entries[len(personalArgs.Entries)-1].Term != rf.curTerm {
					return
				}
				allMatchIndex := []int{}
				for i := range rf.peers {
					if i == rf.me {
						allMatchIndex = append(allMatchIndex, rf.GetLastIndex())
					} else {
						allMatchIndex = append(allMatchIndex, rf.matchIndex[i])
					}
				}
				// DPrintf("arr: %v", allMatchIndex)
				majorityPos := (len(rf.peers) - 1) / 2
				sort.Ints(allMatchIndex)
				min := allMatchIndex[majorityPos]
				DPrintf("[%d]  [%v]'''''''''''''''''''", min, allMatchIndex)
				// 还需要检查min的log的term是否是currentTerm吗
				if min > rf.commitIndex {
					rf.commitIndex = min
					rf.applyCond.Broadcast()
				}
				DPrintf("aaaaaaaa##############################")
			} else {
				// 当前leader失效
				if reply.Term > rf.curTerm {
					// 老了 时代变了 该更新了!!
					// 这里直接更新term 不会影响到接下来的发送(因为数据已经提前构造好了)
					DPrintf("[Change] leader[%d] need to update.....", rf.me)
					rf.rule = Follower
					rf.curTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					return
				}
				// leader有效 但是需要后退
				// 倒退nextIndex 和 matchIndex
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = reply.NextIndex - 1
				DPrintf("server[%d] matchIndex[%d]", server, rf.matchIndex[server])
			}
		}(server)
	}
}

//#######################################################
//#   Applier
//#######################################################
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.GetFirstIndex() {
			// 大概是crash恢复了吧
			rf.lastApplied = rf.GetFirstIndex() // 恢复到之前状态
		}
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		// commitIndex := rf.commitIndex
		// lastApplied := rf.lastApplied
		// firstIndex := rf.GetFirstIndex()
		// DPrintf("server[%d] will apply firstIndex[%d] commitId[%d] applyid[%d]", rf.me, firstIndex, commitIndex, lastApplied)
		// entrties := append([]LogEntry{}, rf.log[lastApplied+1-firstIndex:commitIndex-firstIndex+1]...)
		entrties := []LogEntry{}
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.GetLastIndex() {
			//for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			//DPrintf("[ApplyEntry---] %d apply entry index %d, command %v, term %d, lastSSPindex %d",rf.me,rf.lastApplied,rf.getLogWithIndex(rf.lastApplied).Command,rf.getLogWithIndex(rf.lastApplied).Term,rf.lastSSPointIndex)
			entrties = append(entrties, rf.LogAt(rf.lastApplied))
		}
		rf.mu.Unlock()
		// 这个过程可能通过网络或者阻塞什么的 不能占用锁
		for _, i := range entrties {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      i.Command,
				CommandIndex: i.Index,
			}
			DPrintf("server[%d] apply [%d]", rf.me, i.Index)
		}

		// 更新applyId
		// rf.mu.Lock()
		// // 不止这个协程提交 还有快照 可能自己提交的已经被快照覆盖了吧
		// // commitIndex也可以使用entities的长度进行计算
		// rf.lastApplied = Max(rf.lastApplied, commitIndex)
		// rf.mu.Unlock()
	}
}

//#######################################################
//#   Ticker
//#######################################################
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.rule {
		case Leader:
			go rf.broadCastAppendEnries()
			// 时延
			select {
			case <-time.After(heartBeatInterval):
			case <-rf.appendChan: // 这个应该是有新的command出发发送AppendEntity了 (这样 后期有延时需求)
			}
		case Follower:
			select {
			case <-time.After(time.Millisecond * time.Duration(rand.Int63()%300+200)):
				DPrintf("[%d] timeout and will begin selection round\n", rf.me)
				rf.rule = Candidate
			case <-rf.leaderChan:
				DPrintf(" #####  [%d]  ######\n", rf.me)
			case <-rf.heartsbeats:
				// 没有超时 这里的channel可能会有问题
				// DPrintf("[%d] received heartsbeats....\n", rf.me)
			}
		case Candidate:
			go rf.AttemptElection()
			// 有一种情况 在AttemptElection锁住之前 为别人进行投票并更新到大任期 岂不是相当于站在了巨人肩膀上
			// 无需担心，因为为别人投票就证明自己的数据没有up-to-date 那么别人就会更新自己的任期到更大值
			// 这样的话顶多就是整体任期增大罢了
			select {
			// 下面的时间应该是Election Time 可以比heartbeat time更长一些，因为网络原因
			case <-time.After(time.Duration(rand.Int63()%300+200) * time.Millisecond):
			case <-rf.heartsbeats:
			case <-rf.leaderChan:
				// 这里需要重置nextIndex和matchIndex
				rf.mu.Lock()
				rf.matchIndex = make([]int, len(rf.peers))
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.matchIndex {
					rf.matchIndex[i] = rf.GetLastIndex()
					rf.nextIndex[i] = rf.matchIndex[i] + 1
				}
				rf.mu.Unlock()
			}
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.rule == Leader
	if !isLeader { // 是不是leader直接返回
		return -1, -1, false
	}
	index := rf.GetLastIndex() + 1
	term := rf.curTerm

	// Your code here (2B).
	// 1.构造entity 放进log
	logEntry := LogEntry{
		Index:   index,
		Term:    rf.curTerm,
		Command: command,
	}
	rf.log = append(rf.log, logEntry) // 先放进去 可能提交失败
	rf.persist()
	rf.appendChan <- true
	last := len(rf.log) - 1
	DPrintf("server[%d].log %d %d %v", rf.me, rf.log[last].Index, rf.log[last].Term, rf.log[last].Command)

	// TODO: 如果majority没有成功怎么办
	// 返回失败的话到底算怎么回事

	// 5. return result
	// TODO: 关闭时怎么实现grateful return
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) GetLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}
func (rf *Raft) GetFirstIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) LogAt(index int) LogEntry {
	return rf.log[index-rf.GetFirstIndex()]
}

func outputArgs(where string, server int, args AppendEntityArgs) {
	DPrintf("%s server[%d] :term[%d] leader[%d] prevIndex[%d] prevTerm[%d] entities[%v] leaderCommit[%d]", where, server, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.rule = Follower
	// 下面三个从持久化读取
	rf.curTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		Term:    0,
		Index:   0,
		Command: nil,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))  // 用于leader
	rf.matchIndex = make([]int, len(peers)) // 用于leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.heartsbeats = make(chan bool, 1)
	// rf.lastHeartbeatTime = time.Now().UnixNano()
	rf.leaderChan = make(chan bool, 1)
	rf.appendChan = make(chan bool, 1)

	rf.applyChan = applyCh
	rf.applyCond = sync.NewCond(&rf.mu) // 条件变量是需要锁的
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
