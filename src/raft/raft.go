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

	"log"
	"math/rand"
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

type RaftStatus int

const (
	Follower RaftStatus = iota
	Candidate
	Leader
)

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

	//(2A) code
	role            RaftStatus
	voteFor         int
	votes           int
	term            int
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	//(2B) code
	logs            []LogEntry
	nextIndex       int
	lastCommitIndex int
	applyCh         chan ApplyMsg
	taskRunning     int32
	//
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()

	if rf.role == Leader {
		isleader = true
	}
	term = rf.term

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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	//(2A) code
	Tern      int
	Candidate int
	//

	//(2B) code
	LastCommitIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	//(2A) code
	VoteFor   int
	CurTerm   int
	GetVoting bool
	//

	//(2B) code
	LastCommitIndex int
}

type AppendEntriesArgs struct {
	Leader  int
	CurTerm int

	NextIndex       int
	LastCommitIndex int
	Entrys          []LogEntry

	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesReply struct {
	Leader  int
	CurTerm int

	NextIndex       int
	LastCommitIndex int

	Check bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//心跳 //args.LastCommitIndex<reply.LastCommitIndex这个条件是为了避免掉线的raft不断提升Term导致不响应请求
	if args.CurTerm < rf.term && args.LastCommitIndex < reply.LastCommitIndex {
		//收到过期任期请求
		reply.Leader = rf.voteFor
		reply.CurTerm = rf.term

		reply.NextIndex = rf.nextIndex
		reply.LastCommitIndex = rf.lastCommitIndex
		return
	}
	//一致性检查
	//若两个日志的prevLogIndex和prevLogTerm相等则
	if args.NextIndex == rf.nextIndex {
		if args.PrevLogIndex>0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].Term {
			rf.nextIndex--
		} else {
			if len(args.Entrys) > 0 {
				// log.Printf("raft %v rf.nextIndex %v args.NextIndex %v\n", rf.me, rf.nextIndex, args.NextIndex)
				rf.logs = append(rf.logs, args.Entrys[0])
				rf.nextIndex++
				log.Printf("raft %v logs %v\n", rf.me, rf.logs)
			}
			//触发Commit
			if args.LastCommitIndex > rf.lastCommitIndex {
				if args.LastCommitIndex > len(rf.logs) {
					for i := rf.lastCommitIndex; i < len(rf.logs); i++ {
						msg := ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[i].Command,
							CommandIndex: i + 1,
						}
						rf.applyCh <- msg
						rf.lastCommitIndex++
						log.Printf("raft %v apply logEntry %v\n", rf.me, i+1)

					}
				} else {
					for i := rf.lastCommitIndex; i < args.LastCommitIndex; i++ {
						msg := ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[i].Command,
							CommandIndex: i + 1,
						}
						rf.applyCh <- msg
						rf.lastCommitIndex++
						log.Printf("raft %v apply logEntry %v\n", rf.me, i+1)
					}
				}
			}
		}

	}

	//有日志正在共识

	rf.lastHeartbeat = time.Now()
	rf.role = Follower
	// log.Printf("%v role change %v\n", rf.me, "Follower")
	rf.voteFor = args.Leader
	rf.term = args.CurTerm

	reply.NextIndex = rf.nextIndex
	reply.LastCommitIndex = rf.lastCommitIndex
	reply.Leader = rf.voteFor
	reply.CurTerm = rf.term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//(2A) code
	//只有follower可以投票,任期比现在高才接接受选举

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartbeat = time.Now()

	// log.Printf("raft %v args.Tern= %v rf.term= %v args.LastCommitIndex= %v rf.lastCommitIndex=%v\n",rf.me,args.Tern,rf.term,args.LastCommitIndex,rf.lastCommitIndex)
	//给任期大的投票
	if args.Tern > rf.term && args.LastCommitIndex >= rf.lastCommitIndex {
		rf.role = Follower
		rf.term = args.Tern
		rf.voteFor = args.Candidate

		reply.CurTerm = rf.term
		reply.GetVoting = true
		reply.VoteFor = rf.voteFor
		return
	}

	reply.CurTerm = rf.term
	reply.GetVoting = false
	reply.VoteFor = rf.voteFor
	reply.LastCommitIndex = rf.lastCommitIndex
	//
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	//(2B) code
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.term
	if rf.role != Leader {
		isLeader = false
		return index, term, isLeader
	}

	newLog := LogEntry{
		Term:    term,
		Command: command,
	}

	rf.logs = append(rf.logs, newLog)
	index = len(rf.logs)

	log.Printf("Leader %v had logs %v\n",rf.me,rf.logs)

	rf.nextIndex++

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//(2A) code
		// leader: 发送心跳包
		// follower: 休眠150-300毫秒，超时后检查是否应该成为候选人

		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		electionTimeout := rf.electionTimeout
		lastHeartbeat := rf.lastHeartbeat
		role := rf.role
		rf.mu.Unlock()

		dur := time.Since(lastHeartbeat)

		if role == Follower || role == Candidate {
			//选举超时
			if dur > electionTimeout {
				rf.mu.Lock()

				rf.role = Candidate
				rf.votes = 1
				rf.term++
				rf.voteFor = rf.me
				//重置超时时间
				rf.lastHeartbeat = time.Now()
				// log.Printf("%v role change %v\n", rf.me, "Candidate")

				rf.mu.Unlock()

				//发起请求投票
				wg := &sync.WaitGroup{}
				// log.Printf("raft %v 发起投票 任期%v 且LastCommit=%v \n",rf.me,rf.term,rf.lastCommitIndex)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					wg.Add(1)

					go func(peer int) {
						//在选举超时的时间( <150 ms)内完成投票，避免因单次网络错误导致需要重新选举
						success := int32(0)
						for j := 0; j < 3; j++ {
							go func() {
								ok := sendRequestVoteToPeers(rf, peer)
								if ok {
									atomic.AddInt32(&success, 1)
								}
							}()
							time.Sleep(50 * time.Millisecond)
							//网络正常，且少于50毫秒内完成了rpc请求，则退出重试循环
							if atomic.LoadInt32(&success) > 0 {
								break
							}
						}

						wg.Done()
					}(i)
				}

				wg.Wait()

				//完成投票环节
				rf.mu.Lock()

				//这里判断role是因为发起投票过程可能会收到心跳包，且心跳包的任期比当前节点任期大，导致当前节点role发生改变(变为 Follower)
				//即只允许（候选者）成为（领导者）
				if rf.role == Candidate {
					// log.Printf("term %v: %v get votes %v total %v\n", rf.term, rf.me, rf.votes, len(rf.peers))
					if isWin(rf.votes, len(rf.peers)) {
						rf.role = Leader
						log.Printf("term %v and %v win the leader\n", rf.term, rf.me)
					} else {
						//选举失败重置选举超时
						rf.electionTimeout = randElectionTimeout()
					}
				}
				rf.mu.Unlock()
				continue
			}
		} else if role == Leader {
			//心跳包时间需要长于共识时间
			if dur > 120*time.Millisecond {
				rf.lastHeartbeat = time.Now()

				//发送心跳
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go func(peer int) {
						rf.mu.Lock()

						prevLogIndex := 0
						prevLogTerm := 0
						if rf.nextIndex > 1 {
							prevLogIndex = rf.nextIndex - 1
							prevLogTerm = rf.logs[prevLogIndex-1].Term
						}

						args := &AppendEntriesArgs{
							Leader:          rf.me,
							CurTerm:         rf.term,
							NextIndex:       rf.nextIndex,
							LastCommitIndex: rf.lastCommitIndex,

							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,

							Entrys: nil,
						}
						rf.mu.Unlock()

						reply := &AppendEntriesReply{}

						ok := rf.sendAppendEntries(peer, args, reply)

						if ok {
							
							rf.mu.Lock()

							rfNextIndex := rf.nextIndex
							peerNextIndex := reply.NextIndex

							rf.mu.Unlock()

							if peerNextIndex < rfNextIndex {
								rf.mu.Lock()

								logEntry := &rf.logs[peerNextIndex-1]

								prevLogIndex := 0
								prevLogTerm := 0
								if peerNextIndex > 1 {
									prevLogIndex = peerNextIndex - 1
									prevLogTerm = rf.logs[prevLogIndex-1].Term
								}

								args := &AppendEntriesArgs{
									Leader:          rf.me,
									CurTerm:         logEntry.Term,
									NextIndex:       peerNextIndex,
									LastCommitIndex: rf.lastCommitIndex,
									Entrys:          []LogEntry{*logEntry},

									PrevLogIndex: prevLogIndex,
									PrevLogTerm:  prevLogTerm,
								}
								rf.mu.Unlock()

								reply := &AppendEntriesReply{}
								log.Printf("Leader %v retry send (%v) log to raft %v  prevIndex(%v) and Term(%v)\n", rf.me, peerNextIndex, peer, prevLogIndex, prevLogTerm)
								rf.sendAppendEntries(peer, args, reply)

							}
						}
					}(i)
				}
			}

			//执行共识
			if dur > 100*time.Millisecond {

				rf.mu.Lock()
				lastCommitIndex := rf.lastCommitIndex
				nextIndex := rf.nextIndex
				rf.mu.Unlock()
				// log.Printf("leader %v lastCommitIndex %v nextIndex %v\n", rf.me, lastCommitIndex, nextIndex)
				if lastCommitIndex+1 < nextIndex {
					go func() {

						wg := &sync.WaitGroup{}
						var votes int32 = 1

						rf.mu.Lock()
						entry := &rf.logs[rf.lastCommitIndex]
						index := rf.lastCommitIndex + 1
						rf.mu.Unlock()

						// log.Printf("Leader %v start entry(%v) vote && LastCommit=%v \n",rf.me,entry.Command,index - 1)

						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								continue
							}
							wg.Add(1)
							//发起共识
							go func(peer int, index int) {

								rf.mu.Lock()

								prevLogIndex := 0
								prevLogTerm := 0
								if index > 1 {
									prevLogIndex = index - 1
									prevLogTerm = rf.logs[prevLogIndex-1].Term
								}

								args := &AppendEntriesArgs{
									Leader:          rf.me,
									CurTerm:         entry.Term,
									NextIndex:       index,
									LastCommitIndex: index - 1,
									Entrys:          []LogEntry{*entry},

									PrevLogIndex: prevLogIndex,
									PrevLogTerm:  prevLogTerm,
								}

								reply := &AppendEntriesReply{}
								rf.mu.Unlock()

								ok := rf.sendAppendEntries(peer, args, reply)
								if ok {
									if args.NextIndex+1 == reply.NextIndex {
										atomic.AddInt32(&votes, 1)
									}
								}

								wg.Done()

							}(i, index)
						}

						wg.Wait()

						if isWin(int(votes), len(rf.peers)) {
							rf.mu.Lock()

							if rf.lastCommitIndex < index {
								rf.lastCommitIndex++

								msg := ApplyMsg{
									CommandValid: true,
									Command:      entry.Command,
									CommandIndex: index,
								}
								rf.applyCh <- msg

								log.Printf("raft %v apply logEntry %v logs %v\n", rf.me, index, rf.logs)
							}

							rf.mu.Unlock()
						}
					}()
				}
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

	//(2A) code
	rand.Seed(time.Now().UnixNano())
	rf.role = Follower
	rf.electionTimeout = randElectionTimeout()
	rf.lastHeartbeat = time.Now()
	rf.term = 0
	rf.voteFor = -1
	rf.votes = 0
	log.Printf("%v timeout %v\n", rf.me, rf.electionTimeout)
	//

	//(2B) code
	rf.logs = make([]LogEntry, 0)
	rf.nextIndex = 1
	rf.lastCommitIndex = 0
	rf.applyCh = applyCh
	rf.taskRunning = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func isWin(votes int, total int) bool {
	if votes > total/2 {
		return true
	}
	return false
}

func randElectionTimeout() time.Duration {

	return time.Duration((rand.Intn(150) + 150)) * time.Millisecond
}

//return ：rpc是否请求成功
func sendRequestVoteToPeers(rf *Raft, peer int) bool {
	rf.mu.Lock()

	args := &RequestVoteArgs{
		Tern:            rf.term,
		Candidate:       rf.me,
		LastCommitIndex: rf.lastCommitIndex,
	}

	rf.mu.Unlock()

	reply := &RequestVoteReply{}
	success := rf.sendRequestVote(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if success {
		if reply.GetVoting {
			rf.votes++
			log.Printf("term %v| %v vote to %v\n", rf.term, peer, rf.me)
		}
		if reply.LastCommitIndex > rf.lastCommitIndex || reply.CurTerm > rf.term {
			//发现自己任期已过期,或者本身commit的条目不够
			rf.role = Follower
			rf.voteFor = reply.VoteFor
			rf.term = reply.CurTerm

			// log.Printf("%v role change %v\n", rf.me, "Follower")
		}
	}

	return success
}
