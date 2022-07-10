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

type LogEntry struct {
	Term    int
	Command interface{}
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	PreCandidate
)

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

	//my code
	//Persistent state on all servers:
	//(Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    int

	log []LogEntry

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	//(Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
	//标志：用于阻止其他snapshot rpc（log和snapshot乱序提交）
	needApplySnapshot bool

	applyWork bool
	applyCond *sync.Cond

	//extend
	role              int
	lastTime          time.Time
	electionTimeout   int //mill
	lastSnapshotIndex int
	lastSnapshotTerm  int
	lastHeartBeat     time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()

	if rf.role == int(Leader) {
		isleader = true
	}
	term = rf.currentTerm

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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.lastApplied)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	var lastApplied int
	var currentTerm int
	var votedFor int
	var entries []LogEntry

	if d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&entries) != nil {
		panic("readPersist err")
	} else {
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.lastApplied = lastApplied
		rf.commitIndex = rf.lastApplied
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = entries
		DPrintf("raft %v currentTerm %v voteFor %v log %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	}

}

func (rf *Raft) readSnapshot() {
	rf.lastApplied = rf.lastSnapshotIndex
	rf.commitIndex = rf.lastSnapshotIndex
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

	rf.mu.Lock()
	//由于Snapshot为上层应用调用，而上层应用的Index最多也只是rf.lastApplied
	if index <= rf.lastSnapshotIndex || index > rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	lastSnapshotTerm := rf.log[index-rf.lastSnapshotIndex-1].Term
	lastSnapshotIndex := index

	DPrintf3("raft %v Role %v Term %v 执行快照 Index %v rf.lastSnapshotIndex %v Bebore log %v\n", rf.me, rf.Role(), rf.currentTerm, index, rf.lastSnapshotIndex, rf.log)
	rf.log = rf.log[index-rf.lastSnapshotIndex:]
	DPrintf3("raft %v Role %v Term %v 执行快照 Index %v rf.lastSnapshotIndex %v After log %v\n", rf.me, rf.Role(), rf.currentTerm, index, rf.lastSnapshotIndex, rf.log)

	rf.lastSnapshotTerm = lastSnapshotTerm
	rf.lastSnapshotIndex = lastSnapshotIndex

	//save raft state
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.lastApplied)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	//
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term      int
	LeaderId  int
	LastIndex int
	LastTerm  int
	Data      []byte
}

type InstallSnapshotReply struct {
	Term int
}

//BUG 应该与ApplyLoop越界情况类似2022年5月22日00:58:01
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.killed(){
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIndex <= rf.lastSnapshotIndex || rf.needApplySnapshot {
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.lastTime = time.Now()

	DPrintf3("raft %v Role %v Term %v 提交快照 Index %v rf.lastSnapshotIndex %v Bebore log %v\n", rf.me, rf.Role(), rf.currentTerm, args.LastIndex, rf.lastSnapshotIndex, rf.log)

	if args.LastIndex > rf.lastSnapshotIndex+len(rf.log) {
		rf.log = rf.log[0:0]
	} else {
		rf.log = rf.log[args.LastIndex-rf.lastSnapshotIndex:]
	}

	DPrintf3("raft %v Role %v Term %v 提交快照 Index %v rf.lastSnapshotIndex %v After log %v\n", rf.me, rf.Role(), rf.currentTerm, args.LastIndex, rf.lastSnapshotIndex, rf.log)

	rf.lastSnapshotTerm = args.LastTerm
	rf.lastSnapshotIndex = args.LastIndex

	if rf.lastSnapshotIndex > rf.lastApplied {
		rf.lastApplied = rf.lastSnapshotIndex
	}
	if rf.lastSnapshotIndex >rf.commitIndex{
		rf.commitIndex=rf.lastSnapshotIndex
	}

	//save raft state
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.lastApplied)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	//
	rf.persister.SaveStateAndSnapshot(data, args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIndex,
		SnapshotTerm:  args.LastTerm,
	}

	rf.needApplySnapshot = true
	rf.mu.Unlock()

	rf.applyCh <- msg

	rf.mu.Lock()
	rf.needApplySnapshot = false
	rf.applyWork=true
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return ok
	}
	if args.Term == rf.currentTerm {
		rf.nextIndex[server] = args.LastIndex + 1
		rf.matchIndex[server] = args.LastIndex
		DPrintf3("Leader %v send InstallSnapshot to raft %v nextIndex %v matchIndex %v\n", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
	}

	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//fast backup
	XTerm  int
	XIndex int
	XLen   int
}

//BUG 当前存在会出现Leader set commit，而导致Follower会出现提交错误log
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.Term = rf.currentTerm
	//lab 2D 新增args.PrevLogIndex<rf.lastSnapshotIndex 判断是否请求是否已过期
	if args.Term < rf.currentTerm || args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.Success = false
		return
	}

	rf.lastTime = time.Now()
	rf.electionTimeout = GenElectionTimeout()

	if args.Term > rf.currentTerm {
		DPrintf("raft %v role change Follower in AppendEntries\n", rf.me)
		rf.convertToFollower(args.Term)
	}

	DPrintf3("[%d]: received append entry from [%d],LastApplied %v LastCommit %v,LastSnapshotIndex %v,args term: %d, LeaderCommit: %d, prevLogIndex: %d, prevLogTerm: %d, len(entry): %d entries %v\n raft.log %v\n",
		rf.me, args.LeaderId,rf.lastApplied,rf.commitIndex,rf.lastSnapshotIndex,args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.Entries, rf.log)

	//INFO 处理一致性检查
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > (rf.lastSnapshotIndex+len(rf.log)) || (args.PrevLogIndex > rf.lastSnapshotIndex && args.PrevLogTerm != rf.log[(args.PrevLogIndex-rf.lastSnapshotIndex)-1].Term) {
		reply.Success = false

		//fast backup
		reply.XTerm = -1
		//冲突
		if args.PrevLogIndex > rf.lastSnapshotIndex && args.PrevLogIndex <= (rf.lastSnapshotIndex+len(rf.log)) {
			//找到冲突的Term，便于快速定位
			reply.XTerm = rf.log[(args.PrevLogIndex-rf.lastSnapshotIndex)-1].Term
			//查找对应任期号为XT  erm的第一条Log条目的槽位号
			for i := args.PrevLogIndex; i >= 1; i-- {
				if i == rf.lastSnapshotIndex {
					reply.Success = false
					reply.XTerm = -1
					reply.XLen = args.PrevLogIndex - i + 1
					return
				}
				if rf.log[(i-rf.lastSnapshotIndex)-1].Term == reply.XTerm {
					reply.XIndex = i
				} else {
					break
				}
			}
		}
		//follower缺失log而不是冲突
		if reply.XTerm == -1 {
			reply.XLen = args.PrevLogIndex - (rf.lastSnapshotIndex + len(rf.log))
		}
		return
	}

	if args.PrevLogIndex == rf.lastSnapshotIndex && args.PrevLogTerm != rf.lastSnapshotTerm {
		//令nextIndex回退1
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = 1
		return
	}

	//TIP if an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	//TIP 一致性检查通过后只有存在冲突才进行日志替换
	conflictIndex := -1
	i := 0
	for ; i < len(args.Entries); i++ {
		nowIndex := args.PrevLogIndex + i + 1
		//存在冲突
		DPrintf("nowIndex %v  i %v rf.lastSnapshotIndex %v\n", nowIndex, i, rf.lastSnapshotIndex)
		if nowIndex > (rf.lastSnapshotIndex+len(rf.log)) || rf.log[(nowIndex-rf.lastSnapshotIndex)-1].Term != args.Entries[i].Term {
			conflictIndex = nowIndex - 1
			break
		}
	}

	if conflictIndex != -1 {
		DPrintf("PrevLogIndex %v PrevLogTerm %v\nraft %v recv leader %v log %v and original log %v\n", args.PrevLogIndex, args.PrevLogTerm, rf.me, args.LeaderId, args.Entries, rf.log)
		rf.log = rf.log[:(conflictIndex - rf.lastSnapshotIndex)]
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
		DPrintf3("raft %v Role %v recv Leader %v LeaderTerm %v MyTerm %v Entries Args.Logs %v \n rf.log %v\n", rf.me, rf.Role(), args.LeaderId, args.Term, rf.currentTerm, args.Entries, rf.log)
		DPrintf("Leader %v LTerm %v raft %v MTerm %v passed check and log %v\n", args.LeaderId, args.Term, rf.me, rf.currentTerm, rf.log)
	}

	//TEST args.Term!=rf.currentTerm
	if args.Term != rf.currentTerm {
		panic("args.Term!=rf.currentTerm")
	}

	//deal with commit
	if args.LeaderCommit > rf.commitIndex {
		commit := 0
		if args.LeaderCommit >= (rf.lastSnapshotIndex + len(rf.log)) {
			commit = (rf.lastSnapshotIndex + len(rf.log))
		} else {
			commit = args.LeaderCommit
		}
		if commit-rf.lastSnapshotIndex-1 >= 0 && rf.log[commit-rf.lastSnapshotIndex-1].Term == args.Term {
			rf.commitIndex = commit
			DPrintf3("raft %v %v Term %v Update CommitIndex %v\n", rf.me, rf.Role(), rf.currentTerm, rf.commitIndex)
			//apply
			rf.applyWork = true
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
	return
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	PreVote      bool
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("raft %v want raft %v vote to it\n", args.CandidateId, rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		if !args.PreVote {
			DPrintf("raft %v role change Follower in RequestVote\n", rf.me)
			rf.convertToFollower(args.Term)
		}
	}

	//TIP 2B 选举限制Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。
	//如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
	//如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
	check := false
	reply.VoteGranted = false
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		if len(rf.log) == 0 && rf.lastSnapshotIndex == 0 {
			check = true
		} else if len(rf.log) == 0 && rf.lastSnapshotIndex != 0 {
			if args.LastLogTerm == rf.lastSnapshotTerm && args.LastLogIndex >= rf.lastSnapshotIndex {
				check = true
			} else if args.LastLogTerm > rf.lastSnapshotTerm {
				check = true
			}
		} else if len(rf.log)-1 >= 0 && args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			check = true
		} else if (len(rf.log)-1 >= 0 && args.LastLogTerm == rf.log[len(rf.log)-1].Term) && args.LastLogIndex >= (rf.lastSnapshotIndex+len(rf.log)) {
			check = true
		}
	}
	if check {
		rf.votedFor=args.CandidateId
		reply.VoteGranted = true
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	votedfor := rf.votedFor
	if args.Term > rf.currentTerm {
		votedfor = -1
	}

	//TIP 2B 选举限制Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。
	//如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
	//如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
	check := false
	reply.VoteGranted = false
	if votedfor < 0 || votedfor == args.CandidateId {
		if len(rf.log) == 0 && rf.lastSnapshotIndex == 0 {
			check = true
		} else if len(rf.log) == 0 && rf.lastSnapshotIndex != 0 {
			if args.LastLogTerm == rf.lastSnapshotTerm && args.LastLogIndex >= rf.lastSnapshotIndex {
				check = true
			} else if args.LastLogTerm > rf.lastSnapshotTerm {
				check = true
			}
		} else if len(rf.log)-1 >= 0 && args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			check = true
		} else if (len(rf.log)-1 >= 0 && args.LastLogTerm == rf.log[len(rf.log)-1].Term) && args.LastLogIndex >= (rf.lastSnapshotIndex+len(rf.log)) {
			check = true
		}
	}
	if check {
		reply.VoteGranted = true
	}

	reply.Term = rf.currentTerm
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

func (rf *Raft) sendRequestPreVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestPreVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != int(Leader) {
		isLeader = false
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.persist()

	

	term = rf.currentTerm
	index = rf.nextIndex[rf.me]
	DPrintf3("raft Leader %v recv Index %v log %v\n", rf.me,index,rf.log[len(rf.log)-1])
	
	rf.nextIndex[rf.me] = rf.lastSnapshotIndex + (len(rf.log) + 1)
	rf.matchIndex[rf.me] = rf.lastSnapshotIndex + len(rf.log)

	//lab3 TestSpeed3A需要
	// rf.lastHeartBeat = time.Now()
	//不加go会导致死锁
	// go rf.execHeartBeats()

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

	rf.mu.Lock()
	rf.applyWork = true
	rf.needApplySnapshot = false
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func GenElectionTimeout() int {
	return rand.Intn(500) + 500
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	DPrintf3("raft %v ticker loop start\n", rf.me)
	rf.lastHeartBeat = time.Now()
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		dur := time.Since(rf.lastTime)

		role := rf.role

		electionTimeout := rf.electionTimeout

		rf.mu.Unlock()

		if dur.Milliseconds() > int64(electionTimeout) && role != int(Leader) {
			//选举超时
			rf.mu.Lock()
			// DPrintf3("Raft %v CurTerm %v 选举超时了，并发起选举\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()

			if role == int(Follower) {
				rf.mu.Lock()
				// rf.role = int(Candidate)
				rf.role = int(PreCandidate)
				role = int(PreCandidate)
				rf.mu.Unlock()
			}

			if role == int(PreCandidate) {
				rf.mu.Lock()
				rf.votedFor = rf.me
				//重置选举计时器
				// rf.lastTime = time.Now()
				// rf.electionTimeout = GenElectionTimeout()
				rf.persist()
				rf.mu.Unlock()

				rf.execLeaderPreVote()
			} else if role == int(Candidate) {
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				//重置选举计时器
				rf.lastTime = time.Now()
				rf.electionTimeout = GenElectionTimeout()
				rf.persist()

				// times:=rf.electionTimeout/50
				rf.mu.Unlock()
				//向所有其他服务器发送 RequestVote RPC
				go rf.execLeaderVote()
			}
		}

		rf.mu.Lock()
		lastHeartBeat := rf.lastHeartBeat
		rf.mu.Unlock()

		if time.Since(lastHeartBeat).Milliseconds() > HeartBeatsMill && role == int(Leader) {
			rf.mu.Lock()
			rf.lastHeartBeat = time.Now()

			rf.mu.Unlock()
			rf.execHeartBeats()
		}

		{
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex {
				rf.applyWork = true
				rf.applyCond.Broadcast()
			}
			rf.mu.Unlock()
		}
	}
	DPrintf3("raft %v ticker loop quit\n", rf.me)
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
	rf.applyCh = applyCh
	rf.needApplySnapshot = false

	rf.persister = persister

	rf.applyCond = sync.NewCond(&rf.mu)

	//需要持久化
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	//

	rf.role = int(Follower)
	rf.lastTime = time.Now()
	rf.electionTimeout = GenElectionTimeout()
	DPrintf("raft %v electionTimeout %v\n", rf.me, rf.electionTimeout)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyWork = false
	rf.needApplySnapshot = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf3("raft %v 启动并恢复状态 (LastSnapshotIndex %v LastApplied %v)\n", rf.me, rf.lastSnapshotIndex, rf.lastApplied)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLoop()

	return rf
}

func (rf *Raft) execLeaderVote2(peer int, currentTerm int, lastLogIndex int, lastLogTerm int, votes map[int]int) {

	rf.mu.Lock()
	if rf.role != int(Candidate) {
		rf.mu.Unlock()
		return
	}
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		PreVote:      false,
	}
	reply := &RequestVoteReply{}
	rf.mu.Unlock()

	ok := rf.sendRequestVote(peer, args, reply)

	if ok {
		rf.mu.Lock()
		if args.Term != rf.currentTerm || rf.role != int(Candidate) {
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			DPrintf("raft %v role change Follower in execLeaderVote\n", rf.me)
			rf.convertToFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		if reply.VoteGranted && reply.Term == rf.currentTerm {
			votes[peer] = 1
			DPrintf("Term %v raft %v Got Votes %v Total %v\n", rf.currentTerm, rf.me, votes, len(rf.peers))
			//提前，防止超时变为Follower
			if rf.role == int(Candidate) {
				// DPrintf("raft %v got votes %v\n", rf.me, votes)
				if votesWin(len(votes), len(rf.peers)) {
					rf.convertToLeader()

					//no-op
					// rf.log = append(rf.log, LogEntry{
					// 	Command: nil,
					// 	Term:    rf.currentTerm,
					// })
					// rf.persist()
					// rf.nextIndex[rf.me] = rf.lastSnapshotIndex + (len(rf.log) + 1)
					// rf.matchIndex[rf.me] = rf.lastSnapshotIndex + len(rf.log)

					go rf.execHeartBeats()
					DPrintf("raft %v win become the leader\n", rf.me)
					DPrintf2("raft %v win become the leader\n", rf.me)
					DPrintf3("raft %v CurTerm %v Win become the leader\n", rf.me, rf.currentTerm)
				}
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) execLeaderVote() {

	votes := make(map[int]int, len(rf.peers))
	votes[rf.me] = 1

	rf.mu.Lock()
	if rf.role != int(Candidate) {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	lastLogIndex := rf.lastSnapshotIndex + len(rf.log)
	lastLogTerm := rf.lastSnapshotTerm
	if lastLogIndex > rf.lastSnapshotIndex {
		lastLogTerm = rf.log[(lastLogIndex-rf.lastSnapshotIndex)-1].Term
	}
	DPrintf("raft %v start LeaderVote\n", rf.me)
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.execLeaderVote2(i, currentTerm, lastLogIndex, lastLogTerm, votes)
	}
}

func (rf *Raft) execLeaderPreVote() {
	go func() {
		startTime := 0

		var votes int32 = 1

		rf.mu.Lock()
		if rf.role != int(PreCandidate) {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm + 1
		curterm := rf.currentTerm
		lastLogIndex := rf.lastSnapshotIndex + len(rf.log)
		lastLogTerm := rf.lastSnapshotTerm
		if lastLogIndex > rf.lastSnapshotIndex {
			lastLogTerm = rf.log[(lastLogIndex-rf.lastSnapshotIndex)-1].Term
		}
		electionTimeout := rf.electionTimeout
		// DPrintf("Term %v raft %v start PreVote\n", currentTerm, rf.me)
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(peer, term, curterm, lastLogIndex, lastLogTerm int) {
				rf.mu.Lock()
				if rf.role != int(PreCandidate) {
					rf.mu.Unlock()
					return
				}
				args := &RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
					PreVote:      true,
				}
				reply := &RequestVoteReply{}
				rf.mu.Unlock()

				ok := rf.sendRequestPreVote(peer, args, reply)

				if ok {
					rf.mu.Lock()
					if args.Term != rf.currentTerm+1 || rf.role != int(PreCandidate) {
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.currentTerm {
						DPrintf("raft %v role change Follower in execLeaderVote\n", rf.me)
						rf.convertToFollower(reply.Term)
						rf.mu.Unlock()
						return
					}

					if reply.VoteGranted {
						atomic.AddInt32(&votes, 1)
						DPrintf("PreVote Term %v raft %v Got Votes %v Total %v\n", rf.currentTerm, rf.me, atomic.LoadInt32(&votes), len(rf.peers))
					}

					rf.mu.Unlock()
				}
			}(i, term, curterm, lastLogIndex, lastLogTerm)

		}

		//检测是否赢得选举
		for {
			//提前，防止超时变为Follower
			rf.mu.Lock()
			role := rf.role
			rf.mu.Unlock()
			if role == int(PreCandidate) {
				// DPrintf("raft %v got votes %v\n", rf.me, votes)
				if votesWin(int(atomic.LoadInt32(&votes)), len(rf.peers)) {
					rf.mu.Lock()
					rf.role = int(Candidate)
					rf.mu.Unlock()
					// go rf.execHeartBeats()
					DPrintf3("raft %v win become the PreVote\n", rf.me)
					break
				}
			} else {
				break
			}
			time.Sleep(10 * time.Millisecond)
			startTime += 10
			if startTime > electionTimeout {
				rf.mu.Lock()
				rf.convertToFollower(rf.currentTerm)
				rf.mu.Unlock()
				break
			}
		}
		// DPrintf("currentTerm %v raft %v finished PreVote\n", currentTerm, rf.me)
	}()
}

func (rf *Raft) execHeartBeats2(peer int, currentTerm int, commitIndex int) {
	rf.mu.Lock()
	//避免下面加速回退时，当raft不再为Leader时及时退出，而不是继续发送心跳，
	//导致当当前raft任期被更新后变成Follower后仍发送新任期的心跳包，导致peer以为他是Leader，从而破坏了Peer的日志
	if rf.role != int(Leader) {
		rf.mu.Unlock()
		return
	}

	var entries []LogEntry

	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := -1
	if prevLogIndex >= rf.lastSnapshotIndex {
		if prevLogIndex == rf.lastSnapshotIndex {
			prevLogTerm = rf.lastSnapshotTerm
		} else {

			//BUG lab3A restart出现越界
			//FIX :添加越界判断
			if (prevLogIndex-rf.lastSnapshotIndex)-1 >= len(rf.log) {
				rf.nextIndex[peer]--
				rf.mu.Unlock()
				return
			}
			prevLogTerm = rf.log[(prevLogIndex-rf.lastSnapshotIndex)-1].Term
		}
	} else {
		if rf.nextIndex[peer] <= rf.lastSnapshotIndex {

			go func() {
				rf.mu.Lock()
				if rf.role != int(Leader) {
					rf.mu.Unlock()
					return
				}
				args := &InstallSnapshotArgs{
					Term:      rf.currentTerm,
					LeaderId:  rf.me,
					LastIndex: rf.lastSnapshotIndex,
					LastTerm:  rf.lastSnapshotTerm,
					Data:      rf.ReadSnapshot(),
				}
				reply := &InstallSnapshotReply{}
				rf.mu.Unlock()

				ok := rf.sendInstallSnapshot(peer, args, reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
						rf.mu.Unlock()
						return
					}
					if args.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					if rf.nextIndex[peer] < args.LastIndex+1 {
						rf.nextIndex[peer] = args.LastIndex + 1
					}
					if rf.matchIndex[peer] < args.LastIndex {
						rf.matchIndex[peer] = args.LastIndex
					}

					rf.mu.Unlock()
				}
			}()

			rf.mu.Unlock()
			return
		}
	}

	if (rf.lastSnapshotIndex + len(rf.log)) >= prevLogIndex+1 {
		entries = rf.log[(prevLogIndex - rf.lastSnapshotIndex):]
	}

	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	reply := &AppendEntriesReply{}

	rf.mu.Unlock()

	ok := rf.sendAppendEntries(peer, args, reply)
	if ok {
		rf.mu.Lock()

		if args.Term != rf.currentTerm || rf.role != int(Leader) {
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			DPrintf("raft %v role change Follower in execHeartBeats\n", rf.me)
			rf.convertToFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		/*
			由于RPC在网络中可能乱序或者延迟，我们要确保当前RPC发送时的term、
			当前接收时的currentTerm以及RPC的reply.term三者一致，
			丢弃过去term的RPC，避免对当前currentTerm产生错误的影响。
		*/
		if args.Term == rf.currentTerm && reply.Term == rf.currentTerm {
			if reply.Success {
				// If there exists an N such that N > commitIndex, a majority
				// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
				// set commitIndex = N (§5.3, §5.4).
				rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				DPrintf("Leader %v set raft %v nextIndex %v matchIndex %v\n", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])

				matchIndexs := make([]int, len(rf.peers))
				copy(matchIndexs, rf.matchIndex)
				sort.Ints(matchIndexs)
				DPrintf("Leader %v sortMatchIndex %v\n", rf.me, matchIndexs)
				majoryPos := (len(rf.peers) - 1) / 2

				if (matchIndexs[majoryPos]-rf.lastSnapshotIndex)-1 >= 0 {
					if rf.log[(matchIndexs[majoryPos]-rf.lastSnapshotIndex)-1].Term == rf.currentTerm {
						rf.commitIndex = matchIndexs[majoryPos]
						DPrintf3("raft %v %v Term %v MatchIndex %v Update CommitIndex %v\n", rf.me, rf.Role(), rf.currentTerm, rf.matchIndex, rf.commitIndex)
						rf.applyWork = true
						rf.applyCond.Broadcast()
					}
				}

				DPrintf("Leader %v commitIndex %v\n", rf.me, rf.commitIndex)
			} else {
				//需要快速回退
				//一致性检查失败,nextIndex回退,因rpc可能会重发不可用递减回退
				rf.nextIndex[peer] = prevLogIndex

				if reply.XTerm == -1 {
					rf.nextIndex[peer] = args.PrevLogIndex + 1 - reply.XLen
				} else {
					/*
						Leader发现自己其实有任期XTerm的日志，它会将自己本地记录的Follower的nextIndex设置到本地在XTerm位置的Log条目后面，
						下一次Leader发出下一条AppendEntries时，就可以一次覆盖Follower中槽位2和槽位3对应的Log。
					*/
					for i := rf.nextIndex[peer] - 1; i >= reply.XIndex; i-- {
						//lab 2D 新增,倒退nextIndex时防止越界lastSnapshotIndex
						if i <= rf.lastSnapshotIndex {
							rf.nextIndex[peer]--
							break
							//
						} else {
							if rf.log[(i-rf.lastSnapshotIndex)-1].Term != reply.XTerm {
								rf.nextIndex[peer]--
							} else {
								break
							}
						}
					}
				}

				DPrintf("回退 raft %v nextIndex %v\n", peer, rf.nextIndex[peer])
				//加速回退
				if rf.role == int(Leader) {
					go rf.execHeartBeats2(peer, rf.currentTerm, rf.commitIndex)
				}

			}
		}

		rf.mu.Unlock()
	}
}

//TODO 日志复制(AppendEntries) Last Change: 2022年3月27日16:15:32
func (rf *Raft) execHeartBeats() {
	rf.mu.Lock()
	/*TIP 关于一个导致Apply error出现的问题
	currentTerm必须使用固定值进行rpc，否则在发送一次心跳rpc过程会有不同任期的rpc被发送到其他Peers
	不同的任期会导致AppendEntries处理出错

	例如在Leader1被网络问题隔绝在另一个网络时（即其他peer无法响应它的rpc请求），此时其他Peers已经选出另一位Leader2
	当Leader1恢复网络时，在网络恢复之前发送的心跳rpc会发送给其他Peers,而当这个心跳rpc未发送到Leader2时，Leader1收到其他Peers的回应，并发现
	自己的任期已过期，于是Leader1将自己任期改回最新任期并成为Follower,此时心跳rpc发送到Leader2，Leader2发现任期符合要求，导致Leader2日志被覆盖，
	最终导致Leader apply error
	*/

	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	DPrintf("raft %v send heartbeat\n", rf.me)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.execHeartBeats2(i, currentTerm, commitIndex)
	}
}

func votesWin(votes int, total int) bool {
	if votes > (total / 2) {
		return true
	}
	return false
}

func (rf *Raft) convertToFollower(T int) {
	rf.currentTerm = T
	rf.role = int(Follower)
	rf.lastTime = time.Now()
	rf.electionTimeout = GenElectionTimeout()
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	rf.lastTime = time.Now()
	rf.electionTimeout = GenElectionTimeout()
	rf.role = int(Leader)
	for i := 0; i < len(rf.peers); i++ {
		//初始化所有的 nextIndex 值为自己的最后一条日志的 index 加 1
		rf.nextIndex[i] = rf.lastSnapshotIndex + len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
	DPrintf("Term %v raft %v become Leader\n", rf.currentTerm, rf.me)
}

func (rf *Raft) applyLoop() {
	DPrintf3("raft %v apply loop start\n", rf.me)

	for !rf.killed() {
		rf.mu.Lock()
		for !rf.applyWork || rf.needApplySnapshot {
			rf.applyCond.Wait()
		}
		rf.applyWork = false
		rf.mu.Unlock()

		for {
			rf.mu.Lock()
			if rf.lastApplied >= rf.commitIndex || rf.needApplySnapshot {
				rf.mu.Unlock()
				break
			}
			cmdIndex := rf.lastApplied + 1
			entry := rf.log[(cmdIndex-rf.lastSnapshotIndex)-1]
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: cmdIndex,
				Command:      entry.Command,
			}

			DPrintf3("raft %v %v 提交Log CommandIndex %v CommandTerm %v Command %v (Curent Leader %v Term %v LastApplied %v CommitIndex %v)\n", rf.me, rf.Role(), msg.CommandIndex, entry.Term, msg.Command, rf.votedFor, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			rf.lastApplied = cmdIndex
			rf.persist()
			//先解锁再提交
			rf.mu.Unlock()
			rf.applyCh <- msg
		}
	}

	//quit loop
	//可能存在提交快照前，执行了close(rf.applyCh)导致send on closed channel
	for{
		rf.mu.Lock()
		if !rf.needApplySnapshot{
			close(rf.applyCh)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		time.Sleep(500*time.Millisecond)
	}
	DPrintf3("raft %v apply loop quit\n", rf.me)
}

//lab3B need
func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

//lab3B need
func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}


