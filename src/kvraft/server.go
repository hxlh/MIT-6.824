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

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op  string
	Seq int64
	Cid int64
	Key string
	Val string
	//引入该字段解决当请求超时时协程退出后channel没有接受者的问题
	InTime int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//cid->seq
	seqm map[int64]int64
	//index->ch
	chm map[int]chan interface{}
	//data kv
	data map[string]string
	//lab3B last apply log
	lastApply int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	//seq去重(对Get不处理)
	op := Op{
		Op:  "Get",
		Seq: args.Seq,
		Cid: args.Cid,
		Key: args.Key,
		InTime: time.Now().UnixMilli(),
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Server %v recv Index %v Get key %v\n",kv.me,index,op.Key)

	ch := make(chan interface{}, 0)
	kv.chm[index] = ch

	kv.mu.Unlock()

	ticker := time.NewTicker(RequestTimeout)

	select {
	case <-ticker.C:
		//超时
		//不删除会导致超时后ch无人接受，导致死锁
		reply.Err = ErrWrongLeader
		return
	case c := <-ch:
		rp := c.(GetReply)
		reply.Err = rp.Err
		reply.Value = rp.Value
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()

	//seq去重
	seq := kv.seqm[args.Cid]
	if args.Seq <= seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{
		Op:  args.Op,
		Seq: args.Seq,
		Cid: args.Cid,
		Key: args.Key,
		Val: args.Value,
		InTime: time.Now().UnixMilli(),
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Server %v recv Index %v PutAppend key %v\n",kv.me,index, op.Key)
	ch := make(chan interface{}, 0)
	kv.chm[index] = ch

	kv.mu.Unlock()

	ticker := time.NewTicker(RequestTimeout)

	select {
	case <-ticker.C:
		//超时
		reply.Err = ErrWrongLeader
		return
	case c := <-ch:
		rp := c.(PutAppendReply)
		reply.Err = rp.Err
		return
	}
}

func (kv *KVServer) Put(op *Op) {
	kv.data[op.Key] = op.Val
}

func (kv *KVServer) Append(op *Op) {
	kv.data[op.Key] = kv.data[op.Key] + op.Val
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqm = make(map[int64]int64)
	kv.chm = make(map[int]chan interface{})
	kv.data = make(map[string]string)

	kv.initStatus(persister)

	go kv.applyLoop()

	return kv
}

func (kv *KVServer) initStatus(persister *raft.Persister) {
	DPrintf("Server %v init map %v\n", kv.me,kv.data)
	kv.applySnapshot(kv.rf.ReadSnapshot())

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
		kv.lastApply = lastApplied
		DPrintf("Server %v lastSnapshotIndex %v lastapplied %v len(log) %v\n",kv.me,lastSnapshotIndex,lastApplied,len(entries))
		for i := lastSnapshotIndex+1; i <= lastApplied; i++ {
			op := entries[i-lastSnapshotIndex-1].Command.(Op)
			seq := kv.seqm[op.Cid]
			if op.Seq > seq {
				kv.seqm[op.Cid] = op.Seq
			}else {
				continue
			}

			if op.Op == "Get" {
				continue
			} else if op.Op == "Put" {
				kv.Put(&op)
			} else if op.Op == "Append" {
				kv.Append(&op)
			}
		}
	}
}

func (kv *KVServer) applyLoop() {
	for {
		msg, ok := <-kv.applyCh
		if !ok {
			break
		}
		kv.mu.Lock()
		//最后apply的log

		//apply的日志所有的raft都是相同的
		//msg.Command可能为nil，此时提交的applyMsg是快照

		//lab3B应用快照
		if msg.SnapshotValid {
			if kv.lastApply < msg.SnapshotIndex {
				kv.applySnapshot(msg.Snapshot)
				kv.lastApply = msg.SnapshotIndex
			}
			kv.mu.Unlock()
			continue
		}
		op := msg.Command.(Op)
		seq := kv.seqm[op.Cid]
		kv.lastApply = msg.CommandIndex

		if op.Seq > seq {
			kv.seqm[op.Cid] = op.Seq
		} else if op.Op != "Get" {
			kv.mu.Unlock()
			continue
		}
		
		if _, iL := kv.rf.GetState(); iL {
			//Leader
			var reply interface{}
			if op.Op == "Get" {
				reply=GetReply{
					Err: OK,
					Value: kv.data[op.Key],
				}
				DPrintf("Leader %v exec Index %v Seq %v Get key %v\n",kv.me, msg.CommandIndex, op.Seq, op.Key)
			} else {
				//必须保证PutAppend和seq更新在同一处更新，否则会出现以下丢失log情况
				//错误过程，由于请求有超时时间，如收到apply后更新seq，但在请求协程中处理PutAppend可能会出现seq更新了，但实际上该seq操作（PutAppend）并未执行
				if op.Op == "Put" {
					kv.Put(&op)
				} else {
					kv.Append(&op)
				}
				reply=PutAppendReply{
					Err: OK,
				}
				DPrintf("Leader %v exec Index %v Seq %v PutApeend key %v\n",kv.me,msg.CommandIndex, op.Seq, op.Key)
			}
			//唤醒对应协程
			ch, ok := kv.chm[msg.CommandIndex]
			if ok {
				delete(kv.chm, msg.CommandIndex)
				//接受到请求到Apply的间隔
				dur:=(time.Now().UnixMilli()-op.InTime)*time.Hour.Milliseconds()
				if  dur < int64(RequestTimeout-RequestTimeoutDeviation){
					ch <- reply
				}
			}
		} else {
			//Follower
			if op.Op != "Get" {
				DPrintf("Follower %v exec Index %v Seq %v PutApeend key %v\n",kv.me,msg.CommandIndex, op.Seq, op.Key)
				if op.Op == "Put" {
					kv.Put(&op)
				} else {
					kv.Append(&op)
				}
			}
		}

		//保存快照
		if kv.maxraftstate < kv.rf.GetRaftStateSize() && kv.maxraftstate > 0 {
			w := bytes.NewBuffer([]byte{})
			e := labgob.NewEncoder(w)
			e.Encode(kv.seqm)
			e.Encode(kv.data)
			kv.rf.Snapshot(kv.lastApply, w.Bytes())
		}

		kv.mu.Unlock()
	}
	DPrintf("Server %v quit applyLoop\n",kv.me)
}

func (kv *KVServer) applySnapshot(data []byte) {
	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.seqm)
	d.Decode(&kv.data)
}
