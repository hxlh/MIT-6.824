package shardctrler

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	isDead int32
	//cid -> seq
	seqm map[int64]int64
	//index->ch
	chm map[int]chan interface{}
	//last apply log
	lastApply int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Op  string
	Seq int64
	Cid int64
	// Servers map[int][]string
	// GIDs    []int
	// GID     int
	// Shard   int
	// Num     int
	Option interface{}
	InTime int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()

	//seq去重(优化)
	seq := sc.seqm[args.Cid]
	if args.Seq <= seq {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	op := Op{
		Op:     "Join",
		Cid:    args.Cid,
		Seq:    args.Seq,
		Option: *args,
		InTime: time.Now().UnixMilli(),
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ch := make(chan interface{}, 0)
	sc.chm[index] = ch

	sc.mu.Unlock()

	ticker := time.NewTicker(RequestTimeout)

	select {
	case <-ticker.C:
		//超时
		reply.WrongLeader = true
		return
	case c := <-ch:
		rp := c.(JoinReply)
		reply.WrongLeader = rp.WrongLeader
		reply.Err = rp.Err
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()

	//seq去重(优化)
	seq := sc.seqm[args.Cid]
	if args.Seq <= seq {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	op := Op{
		Op:     "Leave",
		Cid:    args.Cid,
		Seq:    args.Seq,
		Option: *args,
		InTime: time.Now().UnixMilli(),
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ch := make(chan interface{}, 0)
	sc.chm[index] = ch

	sc.mu.Unlock()

	ticker := time.NewTicker(RequestTimeout)

	select {
	case <-ticker.C:
		//超时
		reply.WrongLeader = true
		return
	case c := <-ch:
		rp := c.(LeaveReply)
		reply.WrongLeader = rp.WrongLeader
		reply.Err = rp.Err
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()

	//seq去重(优化)
	seq := sc.seqm[args.Cid]
	if args.Seq <= seq {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	op := Op{
		Op:     "Move",
		Cid:    args.Cid,
		Seq:    args.Seq,
		Option: *args,
		InTime: time.Now().UnixMilli(),
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ch := make(chan interface{}, 0)
	sc.chm[index] = ch

	sc.mu.Unlock()

	ticker := time.NewTicker(RequestTimeout)

	select {
	case <-ticker.C:
		//超时
		reply.WrongLeader = true
		return
	case c := <-ch:
		rp := c.(MoveReply)
		reply.WrongLeader = rp.WrongLeader
		reply.Err = rp.Err
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()

	op := Op{
		Op:     "Query",
		Cid:    args.Cid,
		Seq:    args.Seq,
		Option: *args,
		InTime: time.Now().UnixMilli(),
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ch := make(chan interface{}, 0)
	sc.chm[index] = ch

	sc.mu.Unlock()

	ticker := time.NewTicker(RequestTimeout)

	select {
	case <-ticker.C:
		//超时
		reply.WrongLeader = true
		return
	case c := <-ch:
		rp := c.(QueryReply)
		reply.WrongLeader = rp.WrongLeader
		reply.Err = rp.Err
		reply.Config = rp.Config
		DPrintf("args %v and reply %v\n", args, reply)
		return
	}
}

func (sc *ShardCtrler) join(op *Op) {
	config := sc.newConfig()
	args := op.Option.(JoinArgs)

	for gid, servers := range args.Servers {
		config.Groups[gid] = servers
	}
	//loadblance
	sc.loadBlance(&config)
	DPrintf("join config %v\n", config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) leave(op *Op) {
	config := sc.newConfig()
	args := op.Option.(LeaveArgs)

	for i := 0; i < len(args.GIDs); i++ {
		delete(config.Groups, args.GIDs[i])
	}
	//loadblance
	sc.loadBlance(&config)
	DPrintf("leave gids %v config %v\n", args.GIDs, config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) move(op *Op) {
	config := sc.newConfig()
	args := op.Option.(MoveArgs)
	config.Shards[args.Shard] = args.GID
	DPrintf("move shread %v to gid %v config %v\n", args.Shard, args.GID, config)
	sc.configs = append(sc.configs, config)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.AddInt32(&sc.isDead, 1)
}

func (sc *ShardCtrler) IsDead() bool {
	z := atomic.LoadInt32(&sc.isDead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.isDead = 0
	sc.seqm = make(map[int64]int64)
	sc.chm = make(map[int]chan interface{})

	sc.initStatus(persister)
	go sc.applyLoop()

	return sc
}

func (sc *ShardCtrler) initStatus(persister *raft.Persister) {
	

	data := persister.ReadRaftState()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	var lastApplied int
	var currentTerm int
	var votedFor int
	var entries []raft.LogEntry

	if d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&entries) != nil {
		panic("readPersist err")
	} else {
		sc.lastApply = lastApplied
		DPrintf("Server %v lastSnapshotIndex %v lastapplied %v len(log) %v\n", sc.me, lastSnapshotIndex, lastApplied, len(entries))
		for i := lastSnapshotIndex + 1; i <= lastApplied; i++ {
			op := entries[i-lastSnapshotIndex-1].Command.(Op)
			seq := sc.seqm[op.Cid]
			if op.Seq > seq {
				sc.seqm[op.Cid] = op.Seq
			} else {
				continue
			}

			if op.Op == "Join" {
				sc.join(&op)
			} else if op.Op == "Leave" {
				sc.leave(&op)
			} else if op.Op == "Move" {
				sc.move(&op)
			}
		}
	}
	// DPrintf("ShardCtrler %v init %v\n", sc.me, sc.configs)
}

func (sc *ShardCtrler) applyLoop() {
	for {
		msg, ok := <-sc.applyCh
		if !ok {
			break
		}
		sc.mu.Lock()
		op := msg.Command.(Op)
		seq := sc.seqm[op.Cid]
		sc.lastApply = msg.CommandIndex

		if op.Seq > seq {
			sc.seqm[op.Cid] = op.Seq
		} else if op.Op != "Query" {
			sc.mu.Unlock()
			continue
		}

		if _, iL := sc.rf.GetState(); iL {
			//Leader
			var reply interface{}
			if op.Op == "Query" {
				args := op.Option.(QueryArgs)
				rp := QueryReply{
					WrongLeader: false,
					Err:         OK,
				}
				if args.Num == -1 || args.Num > len(sc.configs)-1 {
					rp.Config = sc.configs[len(sc.configs)-1]
				} else {
					rp.Config = sc.configs[args.Num]
				}
				DPrintf("当前Config %v\n", sc.configs)
				reply = rp
			} else if op.Op == "Join" {
				reply = JoinReply{
					WrongLeader: false,
					Err:         OK,
				}
				sc.join(&op)
			} else if op.Op == "Leave" {
				reply = LeaveReply{
					WrongLeader: false,
					Err:         OK,
				}
				sc.leave(&op)
			} else if op.Op == "Move" {
				reply = MoveReply{
					WrongLeader: false,
					Err:         OK,
				}
				sc.move(&op)
			}
			DPrintf("ShardCtrler Leader %v exec Index %v Seq %v Op %v\n", sc.me, msg.CommandIndex, op.Seq, op.Op)
			//唤醒对应协程
			ch, ok := sc.chm[msg.CommandIndex]
			if ok {
				delete(sc.chm, msg.CommandIndex)
				//接受到请求到Apply的间隔
				dur := (time.Now().UnixMilli() - op.InTime) * time.Hour.Milliseconds()
				if dur < int64(RequestTimeout-RequestTimeoutDeviation) {
					ch <- reply
				}
			}
		} else {
			//Follower
			if op.Op == "Join" {
				sc.join(&op)
			} else if op.Op == "Leave" {
				sc.leave(&op)
			} else if op.Op == "Move" {
				sc.move(&op)
			}
			DPrintf("ShardCtrler Follower %v exec Index %v Seq %v Op %v\n", sc.me, msg.CommandIndex, op.Seq, op.Op)
		}

		sc.mu.Unlock()
	}
	DPrintf("ShardCtrler %v quit applyLoop\n", sc.me)
}

func (sc *ShardCtrler) newConfig() Config {
	oldCfg := sc.configs[len(sc.configs)-1]
	newCfg := oldCfg
	newCfg.Num++
	newCfg.Groups = map[int][]string{}

	for gid, servers := range oldCfg.Groups {
		newCfg.Groups[gid] = servers
	}
	return newCfg
}

func (sc *ShardCtrler) loadBlance(config *Config) {
	arr := []int{}
	for k := range config.Groups {
		arr = append(arr, k)
	}

	if len(arr) == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}

	sort.Ints(arr)
	d := NShards % len(arr)
	cnt := NShards / len(arr)
	index := 0
	for _, v := range arr {
		for i := 0; i < cnt; i++ {
			config.Shards[index] = v
			index++
		}
		if d > 0 {
			config.Shards[index] = v
			index++
			d--
		}
	}
}
