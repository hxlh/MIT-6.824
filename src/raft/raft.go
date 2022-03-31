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

type LogEntry struct {
	Term    int
	Command interface{}
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
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

	//extend
	role            int
	lastTime        time.Time
	electionTimeout int //mill
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	//INFO 处理一致性检查
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > len(rf.log) || (args.PrevLogIndex > 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term) {
		reply.Success = false

		//fast backup
		reply.XTerm = -1
		//冲突
		if args.PrevLogIndex > 0 && args.PrevLogIndex <= len(rf.log) {
			//找到冲突的Term，便于快速定位
			reply.XTerm = rf.log[args.PrevLogIndex-1].Term
			//查找对应任期号为XTerm的第一条Log条目的槽位号
			for i := args.PrevLogIndex; i >= 1; i-- {
				if rf.log[i-1].Term == reply.XTerm {
					reply.XIndex = i
				} else {
					break
				}
			}
		}
		//follower缺失log而不是冲突
		if reply.XTerm == -1 {
			reply.XLen = args.PrevLogIndex - len(rf.log)
		}
		return
	}

	//TODOIf an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if len(args.Entries)>0 {
		rf.log=rf.log[:args.PrevLogIndex]
		rf.log = append(rf.log, args.Entries...)
	}

	//deal with commit
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit >= len(rf.log) {
			rf.commitIndex = len(rf.log)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//apply
		go func() {
			rf.mu.Lock()
			entries := rf.log
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			for i := rf.lastApplied + 1; i <= commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      entries[i-1].Command,
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied = i
				rf.mu.Unlock()
			}

			// rf.mu.Lock()
			// DPrintf("raft %v log %v\n", rf.me, rf.log)
			// rf.mu.Unlock()
		}()

	}

	//更新选举计时器
	rf.lastTime = time.Now()
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
		rf.convertToFollower(args.Term)
	}

	//TIP 2B 选举限制Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。
	//如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
	//如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && (len(rf.log) == 0 ||
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log))) {

		DPrintf("raft %v vote to %v\n", rf.me, args.CandidateId)
		rf.lastTime = time.Now()
		rf.votedFor = args.CandidateId
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

	// DPrintf("raft %v recv log %v\n", rf.me, rf.log)

	term = rf.currentTerm
	index = rf.nextIndex[rf.me]

	rf.nextIndex[rf.me] = len(rf.log) + 1
	rf.matchIndex[rf.me] = len(rf.log)

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

func GenElectionTimeout() int {
	return rand.Intn(150) + 150
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	lastHeartBeat := time.Now()
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
			if role == int(Follower) {
				rf.mu.Lock()
				rf.role = int(Candidate)
				rf.mu.Unlock()
			}

			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			//重置选举计时器
			rf.lastTime = time.Now()
			rf.electionTimeout = GenElectionTimeout()
			rf.mu.Unlock()
			//向所有其他服务器发送 RequestVote RPC
			go rf.execLeaderVote()
		}

		if time.Since(lastHeartBeat).Milliseconds() > 100 {
			rf.mu.Lock()
			role := rf.role
			rf.mu.Unlock()
			if role == int(Leader) {
				lastHeartBeat = time.Now()
				rf.execHeartBeats()
			}
		}

	}
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

	//mycode
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = int(Follower)
	rf.lastTime = time.Now()
	rf.electionTimeout = GenElectionTimeout()
	DPrintf("raft %v electionTimeout %v\n", rf.me, rf.electionTimeout)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) execLeaderVote() {
	votes := 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()

			lastLogIndex := len(rf.log)
			lastLogTerm := -1
			if lastLogIndex > 0 {
				lastLogTerm = rf.log[lastLogIndex-1].Term
			}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
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
					rf.convertToFollower(reply.Term)
				}

				if reply.VoteGranted && reply.Term == rf.currentTerm {
					votes++
					//提前，防止超时变为Follower
					if rf.role == int(Candidate) {
						DPrintf("raft %v got votes %v\n", rf.me, votes)
						if votesWin(votes, len(rf.peers)) {
							rf.convertToLeader()
							// go rf.execHeartBeats()
							DPrintf("raft %v win become the leader\n", rf.me)
						}
					}
				}

				rf.mu.Unlock()
			}
		}(i)
	}
}

//TODO 日志复制(AppendEntries) Last Change: 2022年3月27日16:15:32
func (rf *Raft) execHeartBeats() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {

			rf.mu.Lock()

			var entries []LogEntry
			prevLogIndex := rf.nextIndex[peer] - 1
			prevLogTerm := -1
			if prevLogIndex > 0 {
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}

			if len(rf.log) >= rf.nextIndex[peer] {
				entries = rf.log[rf.nextIndex[peer]-1:]
			}

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
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
					rf.convertToFollower(reply.Term)
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
						rf.nextIndex[peer] = args.PrevLogIndex + len(entries) + 1
						rf.matchIndex[peer] = args.PrevLogIndex + len(entries)

						matchIndexs := make([]int, len(rf.peers))
						copy(matchIndexs, rf.matchIndex)
						sort.Ints(matchIndexs)
						majoryPos := (len(rf.peers) - 1) / 2
						for i := majoryPos; i >= 0 && matchIndexs[i] > rf.commitIndex; i-- {
							if rf.log[matchIndexs[i]-1].Term == rf.currentTerm {
								rf.commitIndex = matchIndexs[i]

								//apply msg
								go func() {
									rf.mu.Lock()
									commitIndex := rf.commitIndex
									lastApplyed := rf.lastApplied
									ens := rf.log
									rf.mu.Unlock()

									for i := lastApplyed + 1; i <= commitIndex; i++ {
										msg := ApplyMsg{
											CommandValid: true,
											CommandIndex: i,
											Command:      ens[i-1].Command,
										}
										rf.applyCh <- msg
										rf.mu.Lock()
										rf.lastApplied = i
										rf.mu.Unlock()
									}
									// rf.mu.Lock()
									// DPrintf("Leader %v log %v\n", rf.me, rf.log)
									// rf.mu.Unlock()
								}()
							}
						}

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
								if rf.log[i-1].Term != reply.XTerm {
									rf.nextIndex[peer]--
								} else {
									break
								}
							}
						}

						DPrintf("回退 raft %v nextIndex %v\n", peer, rf.nextIndex[peer])
					}
				}

				rf.mu.Unlock()
			}
		}(i)
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
	rf.votedFor = -1
	DPrintf("raft %v change to follower\n", rf.me)
}

func (rf *Raft) convertToLeader() {
	rf.lastTime = time.Now()
	rf.role = int(Leader)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
}
