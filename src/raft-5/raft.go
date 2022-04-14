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

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

const heartBeatInterval = 50 * time.Millisecond

type LogEntry struct {
	Term    int
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

	heartsbeats       chan bool // 别人给自己发送heartbeat
	lastHeartbeatTime time.Time // 防止断联之后大term选举
	leaderChan        chan bool // 自己成为leader之后要立马通知还在slect timeout 的ticker
	appendChan        chan bool // leader发送appendEnties之后就不需要再发送heartbest了
	applyChan         chan ApplyMsg
}

func Min(a int, b int) int {
	if a < b {
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 有一个现象就是断联之后的节点恢复之后会以大term出发选举，但是有不满足up to date的log
	if time.Now().Before(rf.lastHeartbeatTime.Add(heartBeatInterval)) {
		DPrintf("[%d] is normal, so can not vote for [%d]\n", rf.me, args.CandidateId)
		return
	}
	if rf.rule == Leader {
		DPrintf("[%d] is leader, so can not vote for [%d]\n", rf.me, args.CandidateId)
		return
	}
	DPrintf("[%d] received the voteRequest from [%d]\n", rf.me, args.CandidateId)
	// 1. 选举的前提之一
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm // 只有在没有选举成功的时候才需要返回term
		reply.VoteGranted = false
		return
	}

	// 如果term 更高 直接投票 并改变自己的term 以后遇到更高的term继续改变即可(骑驴找马)
	if rf.curTerm < args.Term {
		rf.rule = Follower
		rf.curTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.heartsbeats <- true
		DPrintf("[RequestVote]:[%d] vote for: [%d] (bigger term)", rf.me, args.CandidateId)
		return
	}

	// 2. 判断条件(此时rf.curTerm == args.term)
	selfLastLogIndex := len(rf.log) - 1
	selfLastLogTerm := -1
	if selfLastLogIndex != -1 {
		selfLastLogTerm = rf.log[selfLastLogIndex].Term
	}
	if args.LastLogTerm < selfLastLogTerm ||
		args.LastLogTerm == selfLastLogTerm && args.LastLogIndex < selfLastLogIndex {
		reply.VoteGranted = false
		reply.Term = rf.curTerm
		// 此时rf.curTerm == args.term
		DPrintf("[RequestVote]:[%d] can not vote for: [%d] (not up to date)", rf.me, args.CandidateId)
		return
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 在经历众多考验之后 如果没有投票或者投票人就是这个server时 这里不太明白 为什么不直接投票
		rf.rule = Follower
		rf.votedFor = args.CandidateId
		// rf.curTerm == args.term 隐含
		reply.VoteGranted = true
		rf.heartsbeats <- true
		DPrintf("[RequestVote]:[%d] vote for: [%d] (up to date log)", rf.me, args.CandidateId)
	}
}

//#######################################################
//#   AttemptElection
//#######################################################
func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	rf.curTerm++
	DPrintf("[%d] begin election in term --- [%d] ---\n", rf.me, rf.curTerm)
	rf.rule = Candidate
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term:         rf.curTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  -1,
	}
	if args.LastLogIndex != -1 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
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
			if !reply.VoteGranted {
				// DPrintf("[%d] can not get vote from [%d]\n", rf.me, server)
				if reply.Term > rf.curTerm {
					DPrintf("[%d] received higher term so chenged to follower\n", rf.me)
					rf.curTerm = reply.Term
					rf.rule = Follower
				}
				return
			}

			// 判断不能是 Follower 因为可能在这个过程中可能已经有leader了
			// 按道理来说 不应该出现 但是可能有candidate进入新的选举任期并
			if rf.rule == Follower || rf.rule == Leader {
				return // 已经产生leader了
			}
			atomic.AddInt64(&vote, 1) // 这里可以使用原子操作
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
//#   Ticker
//#######################################################
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		selfRule := rf.rule
		rf.mu.Unlock()
		if selfRule == Leader {
			select {
			case <-time.After(heartBeatInterval):
			case <-rf.leaderChan:
			case <-rf.appendChan: // 因为append就不需要再发送heartbeat包了
				continue
			}

			// 发送心跳包呀
			rf.mu.Lock()

			args := AppendEntityArgs{
				Term:         rf.curTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: -1,  // 根据server定制化
				PrevLogTerm:  -1,  // 根据server定制化
				Entries:      nil, // 根据server定制化
			}
			rf.mu.Unlock()
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				go func(server int) {
					rf.mu.Lock()
					// 心跳包也需要带上leaderCommitIndex和没有完成的log
					// DPrintf("[%d] send hearteat to [%d]\n", args.LeaderId, server)
					personalArgs := args
					personalArgs.PrevLogIndex = rf.matchIndex[server]
					if personalArgs.PrevLogIndex != -1 {
						personalArgs.PrevLogTerm = rf.log[personalArgs.PrevLogIndex].Term
					}
					if personalArgs.PrevLogIndex < len(rf.log)-1 {
						personalArgs.Entries = rf.log[rf.nextIndex[server]:]
					}
					// outputArgs("heartbeat", server, personalArgs)
					rf.mu.Unlock()
					// 计算自己需要的起始位置
					reply := AppendEntityReply{}
					ok := rf.peers[server].Call("Raft.AppendEntity", &personalArgs, &reply)
					if ok {
						DPrintf("[%d] received the reply of heartbeat from [%d]", args.LeaderId, server)
					}
					// 每次发完都要检查一下是否可以更新nextIndex和matchIndex 以及 commitIndex
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// DPrintf("real result [%d] %v\n", server, reply)
					if reply.Success {
						if reply.NoAction {
							return // 没有进行什么动作
						}
						// 增大nextIndex matchIndex 判断N那个 更新commitIndex
						DPrintf("server[%d] before: nextIndex[%d] matchIndex[%d]", server, rf.nextIndex[server], rf.matchIndex[server])
						rf.nextIndex[server] = 1 + personalArgs.PrevLogIndex + len(personalArgs.Entries)
						rf.matchIndex[server] = personalArgs.PrevLogIndex + len(personalArgs.Entries)
						DPrintf("server[%d] after: nextIndex[%d] matchIndex[%d]", server, rf.nextIndex[server], rf.matchIndex[server])
						// 如何更新commitIndex 需要大多数的follower都有了才行
						allMatchIndex := []int{}
						for i := range rf.peers {
							if i == rf.me {
								allMatchIndex = append(allMatchIndex, len(rf.log)-1)
							} else {
								allMatchIndex = append(allMatchIndex, rf.matchIndex[i])
							}
						}
						// DPrintf("arr: %v", allMatchIndex)
						majorityPos := (len(rf.peers) - 1) / 2
						sort.Ints(allMatchIndex)
						min := sort.IntSlice(allMatchIndex)[majorityPos]
						DPrintf("[%d]  [%v]'''''''''''''''''''", min, allMatchIndex)
						if min > rf.commitIndex {
							for i := rf.commitIndex + 1; i <= min; i++ {
								rf.applyChan <- ApplyMsg{
									CommandValid: true,
									Command:      rf.log[i].Command,
									CommandIndex: i,
								}
							}
							rf.commitIndex = min // min 肯定在[old_next_index, new_next_index)
							rf.lastApplied = rf.commitIndex
						}
					} else if args.Entries != nil {
						// 倒退nextIndex 和 matchIndex
						rf.nextIndex[server] -= 1
						rf.matchIndex[server] -= 1
						if rf.matchIndex[server] < -1 || rf.nextIndex[server] < 0 {
							DPrintf("server [%d]'s nextIndex and matchIndex invalid(-1)\n", server)
						}
					}
				}(server)
			}
		} else {
			rand.Seed(time.Now().UnixNano())
			randTime := rand.Intn(200) + 200
			// DPrintf("[%d][%d]\n", rf.me, randTime)
			select {
			case <-time.After(time.Millisecond * time.Duration(randTime)):
				DPrintf("[%d] timeout and will begin selection round\n", rf.me)
				// rf.mu.Lock()
				// DPrintf("aaaaaa")
				rf.AttemptElection()
			case <-rf.leaderChan:
				DPrintf(" #####  [%d]  ######\n", rf.me)
			case <-rf.heartsbeats:
				// 没有超时 这里的channel可能会有问题
				// DPrintf("[%d] received information....\n", rf.me)
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
func outputArgs(where string, server int, args AppendEntityArgs) {
	DPrintf("%s server[%d] :term[%d] leader[%d] prevIndex[%d] prevTerm[%d] entities[%v] leaderCommit[%d]", where, server, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.rule == Leader
	if !isLeader { // 是不是leader直接返回
		return -1, -1, false
	}
	index := len(rf.log)
	term := rf.curTerm

	// Your code here (2B).
	// 1.构造entity 放进log
	logEntry := LogEntry{
		Term:    rf.curTerm,
		Command: command,
	}
	rf.log = append(rf.log, logEntry) // 先放进去 可能提交失败
	rf.nextIndex[rf.me] = len(rf.log)
	// // 2. 发送到followers
	// args := AppendEntityArgs{
	// 	Term:         rf.curTerm,
	// 	LeaderId:     rf.me,
	// 	PrevLogIndex: -1, // 根据follower的情况来
	// 	PrevLogTerm:  -1, // 根据follower的情况来
	// 	Entries:      nil,
	// 	LeaderCommit: rf.commitIndex,
	// }
	// rf.mu.Unlock()
	// // 统计成功返回数目
	// var okCount int64 = 1
	// wg := sync.WaitGroup{}
	// for server := range rf.peers {
	// 	if server == rf.me {
	// 		continue
	// 	}
	// 	wg.Add(1)
	// 	go func(server int) {
	// 		defer wg.Done()
	// 		rf.mu.Lock()
	// 		personalArgs := args
	// 		personalArgs.PrevLogIndex = rf.matchIndex[server] // matchIndex和nextIndex-1有什么区别
	// 		if personalArgs.PrevLogIndex == -1 {
	// 			personalArgs.PrevLogTerm = -1
	// 		} else {
	// 			personalArgs.PrevLogTerm = rf.log[personalArgs.PrevLogIndex].Term
	// 		}
	// 		// 计算自己需要的起始位置
	// 		personalArgs.Entries = rf.log[rf.nextIndex[server]:]
	// 		// outputArgs("Start", server, personalArgs)
	// 		rf.mu.Unlock()
	// 		reply := AppendEntityReply{}
	// 		ok := rf.peers[server].Call("Raft.AppendEntity", &personalArgs, &reply)
	// 		if !ok {
	// 			DPrintf("Send entry to follower Failed leader[%d] => follower[%d] index[%d] command[%v]\n", rf.me, server, personalArgs.LeaderId+1, personalArgs.Entries)
	// 			return
	// 		}
	// 		DPrintf("[%d] succeed [%v] ===============]", server, command)
	// 		rf.mu.Lock()
	// 		defer rf.mu.Unlock()
	// 		if !reply.Success {
	// 			// 这里的reply.Term是什么
	// 			if reply.Term > rf.curTerm { // 不可能出现
	// 				DPrintf("[Error Error Error] #####  AppedEntries reply.term > leader.Term\n")
	// 			}
	// 			// 2. 还是位置提示 更新 nextIndex数目和matchIndex数组
	// 			rf.nextIndex[server] -= 1
	// 			rf.matchIndex[server] -= 1
	// 			if rf.matchIndex[server] < -1 || rf.nextIndex[server] < 0 {
	// 				DPrintf("[Error Error Error] ### back too much\n")
	// 			}
	// 			return
	// 		}
	// 		atomic.AddInt64(&okCount, 1)
	// 		if reply.NoAction {
	// 			DPrintf("server[%d]NoAction  in Start", server)
	// 			return
	// 		}
	// 		// 这里更新nextIndex[server] 和 matchIndex[server]
	// 		// 以下需要保证
	// 		// assert(rf.matchIndex[server] == rf.matchIndex[rf.me])
	// 		// assert(rf.nextIndex[server] == rf.nextIndex[rf.me])
	// 		if rf.matchIndex[server] > personalArgs.PrevLogIndex+len(personalArgs.Entries) {
	// 			return
	// 		}
	// 		if rf.nextIndex[server] > 1+personalArgs.PrevLogIndex+len(personalArgs.Entries) {
	// 			return
	// 		}
	// 		DPrintf("server[%d] before: nextIndex[%d] matchIndex[%d]", server, rf.nextIndex[server], rf.matchIndex[server])
	// 		rf.matchIndex[server] = personalArgs.PrevLogIndex + len(personalArgs.Entries)
	// 		rf.nextIndex[server] = 1 + personalArgs.PrevLogIndex + len(personalArgs.Entries)
	// 		DPrintf("server[%d] after: nextIndex[%d] matchIndex[%d]", server, rf.nextIndex[server], rf.matchIndex[server])
	// 	}(server)
	// }
	// // 等待所有结束
	// wg.Wait()
	// DPrintf("[%v]===========================================================\n", command)
	// // 3. 接收到majority回复后commit log entity
	// if int(atomic.LoadInt64(&okCount)) >= len(rf.peers)/2+1 {
	// 	// 提交即可
	// 	rf.mu.Lock()

	// 	allMatchIndex := []int{}
	// 	for i := range rf.peers {
	// 		if i == rf.me {
	// 			allMatchIndex = append(allMatchIndex, len(rf.log)-1)
	// 		} else {
	// 			allMatchIndex = append(allMatchIndex, rf.matchIndex[i])
	// 		}
	// 	}
	// 	// DPrintf("arr: %v", allMatchIndex)
	// 	majorityPos := (len(rf.peers) - 1) / 2
	// 	min := sort.IntSlice(allMatchIndex)[majorityPos]
	// 	if min > rf.commitIndex {
	// 		for i := rf.commitIndex + 1; i <= min; i++ {
	// 			rf.applyChan <- ApplyMsg{
	// 				CommandValid: true,
	// 				Command:      rf.log[i].Command,
	// 				CommandIndex: i,
	// 			}
	// 		}
	// 		rf.commitIndex = min // min 肯定在[old_next_index, new_next_index)
	// 		rf.lastApplied = rf.commitIndex
	// 	}

	// 	// rf.commitIndex = len(rf.log) - 1
	// 	// 在心跳中还要继续发送 更新commitIndex就需要统计matchIndex有没有majority了
	// 	// AAA: If there exists an N such that N > commitIndex, a majority
	// 	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// 	// set commitIndex = N

	// 	// 4. apply to machine
	// 	// if rf.lastApplied < rf.commitIndex {
	// 	// 	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
	// 	// 		rf.applyChan <- ApplyMsg{
	// 	// 			CommandValid: true,
	// 	// 			Command:      rf.log[i].Command,
	// 	// 			CommandIndex: i,
	// 	// 		}
	// 	// 	}
	// 	// }
	// 	// rf.lastApplied = rf.commitIndex
	// 	rf.mu.Unlock()
	// }
	// TODO: 如果majority没有成功怎么办
	// 返回失败的话到底算怎么回事

	// 5. return result
	// TODO: 怎么实现grateful return
	return index, term, isLeader
}

// 大问题 matchIndex nextIndex怎么做 TODO:
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

//#######################################################
//#   AppendEntity
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
	Term     int // currentTerm, for leader to update itself
	Success  bool
	NoAction bool // 没有append(可能是并发访问) 就不需要更新nextIndex和matchIndex了
}

// 有个问题 就是在选举完leader之后需不需要更新什么log的信息(nextIndex matchIndex)
func (rf *Raft) AppendEntity(args *AppendEntityArgs, reply *AppendEntityReply) {
	// 这里被远程调用
	// DPrintf("   !!  [%d]\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.heartsbeats <- true // 有了信号就可以重置了 这里进行有时会阻塞

	if args.Term < rf.curTerm {
		DPrintf("arg.term < rf.curterm")
		reply.Success = false
		return
	}
	//---------------------------------------------------------------
	rf.lastHeartbeatTime = time.Now()
	outputArgs("AppendEntity", rf.me, *args)
	// 判断是不是heartbeat
	if args.Entries == nil {
		// DPrintf("[%d] received the heartbeat from [%d]       ## [%d] ##\n", rf.me, args.LeaderId, args.Term)
		// 可能自己错过了什么
		rf.rule = Follower
		rf.curTerm = args.Term
		// DPrintf("[%d] old old old ----------------------------------------------------\n", rf.me)
		// 不清楚为什么会出现下面的情况
		for {
			if len(rf.heartsbeats) > 0 {
				<-rf.heartsbeats
			} else {
				break
			}
		}
	}
	// ----------------------------------------------------
	rf.heartsbeats <- true
	// 更新commitIndex 因为commit的结果在下一次发送中被告知follower
	if rf.commitIndex < args.LeaderCommit {
		// 需要更新了
		old_commitIndex := rf.commitIndex
		// DPrintf("aaa need to update commitIndex oldIndex[%d]  len(log)[%d] leaderIndex[%d]\n", old_commitIndex, len(rf.log), args.LeaderCommit)
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		// 可能有很多需要commit 和 apply
		// DPrintf("[%d] old_commitIndex[%d] rf.commitIndex[%d]\n", rf.me, old_commitIndex, rf.commitIndex)
		for i := old_commitIndex + 1; i <= rf.commitIndex; i++ {
			DPrintf("||||||||||||||  server[%d] apply[%d] ||||||||", rf.me, i)
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.lastApplied = i
		}

	} else if rf.commitIndex > args.LeaderCommit {
		return // 不可能吧
	}
	if args.Entries == nil {
		return
	}
	// ------------------------------------------------------
	// DPrintf("aaaaaaaaaaaaaaaaaaaaaaaaaaa\n")
	// 判断是否匹配
	// 1. 是否有
	if len(rf.log) <= args.PrevLogIndex {
		// 需要回退一步
		reply.Success = false
		return
	}
	// 2.是否match
	if args.PrevLogIndex != -1 &&
		args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		// delete entries from prevLogIndex
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Success = false // 需要回退
		return
	}
	// 3. 因为是并发 所以可能出现少覆盖多的情况 如果长度不短于更新后的
	// 并且最后一个entity.term是当前term 就不需要添加了
	if len(rf.log)-1 >= args.PrevLogIndex+len(args.Entries) &&
		args.Term == rf.log[len(rf.log)-1].Term {
		reply.Success = true
		reply.NoAction = true
		DPrintf("server[%d] NoAction.................................", rf.me)
		return
	}
	rf.log = rf.log[:args.PrevLogIndex+1] // 先把后边无用的去掉

	// 终于可以append了AppendEntity
	// DPrintf("append..............................")
	for _, entity := range args.Entries {
		rf.log = append(rf.log, entity)
	}
	// 更新commitIndex 因为commit的结果在下一次发送中被告知follower
	if rf.commitIndex < args.LeaderCommit {
		// 需要更新了
		old_commitIndex := rf.commitIndex
		DPrintf("server[%d] need to update commitIndex oldIndex[%d]  len(log)[%d] leaderIndex[%d]\n", rf.me, old_commitIndex, len(rf.log), args.LeaderCommit)
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		// 可能有很多需要commit 和 apply
		for i := old_commitIndex + 1; i <= rf.commitIndex; i++ {
			DPrintf("||||||||||||||  server[%d] apply[%d] ||||||||", rf.me, i)
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.lastApplied = i
		}
	} else if rf.commitIndex > args.LeaderCommit {
		return // 不需要更新了 虽然不太能解释清除 但是好像确实没问题
	}
	DPrintf("server[%d] receivede the log [%v] commitIndex[%d] len(log)[%v]\n", rf.me, args.Entries, rf.commitIndex, rf.log)
	reply.Success = true
	reply.NoAction = false
	reply.Term = 100 // 这里的term 是用于什么? TODO:
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

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))  // 用于leader
	rf.matchIndex = make([]int, len(peers)) // 用于leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
	}

	rf.heartsbeats = make(chan bool, 1)
	rf.lastHeartbeatTime = time.Now()
	rf.leaderChan = make(chan bool, 1)
	rf.appendChan = make(chan bool, 1)
	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
