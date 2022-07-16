package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

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
	InTime   int64
	UserData interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck  *shardctrler.Clerk
	dead int32 // set by Kill()
	//cid->seq
	seqm map[int64]int64
	//index->ch
	chm map[int]chan interface{}
	//data kv
	data map[string]string
	//lab3B last apply log
	lastApply int
	//config
	config shardctrler.Config
	//
	cid int64
	seq int64
	//shards config
	shardsToCfgNum map[int]int
	durFetchShards int32
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.shardsToCfgNum[shard] != args.ConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	//seq去重(对Get不处理)
	op := Op{
		Op:       "Get",
		Seq:      args.Seq,
		Cid:      args.Cid,
		Key:      args.Key,
		InTime:   time.Now().UnixMilli(),
		UserData: args.ConfigNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Server %v recv Index %v Cid %v Seq %v PutAppend key %v Val %v ConfigNum %v Shard %v SrvConfigToNum %v\n", kv.gid, index, args.Cid, args.Seq, op.Key, op.Val, args.ConfigNum, shard, kv.shardsToCfgNum)

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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//根据config过滤
	shard := key2shard(args.Key)
	if kv.shardsToCfgNum[shard] != args.ConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	//seq去重
	seq := kv.seqm[args.Cid]
	if args.Seq <= seq {
		DPrintf("ShardKV %v Me %v recv 被过滤 Cid %v Seq %v PutAppend key %v Val %v And Now Seq %v\n", kv.gid, kv.me, args.Cid, args.Seq, args.Key, args.Value, seq)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{
		Op:       args.Op,
		Seq:      args.Seq,
		Cid:      args.Cid,
		Key:      args.Key,
		Val:      args.Value,
		InTime:   time.Now().UnixMilli(),
		UserData: args.ConfigNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Server %v Me %v recv Index %v Cid %v Seq %v PutAppend key %v Val %v ConfigNum %v Shard %v SrvConfigToNum %v\n", kv.gid, kv.me, index, args.Cid, args.Seq, op.Key, op.Val, args.ConfigNum, shard, kv.shardsToCfgNum)
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

func (kv *ShardKV) Put(op *Op) {
	kv.data[op.Key] = op.Val
}

func (kv *ShardKV) Append(op *Op) {
	kv.data[op.Key] = kv.data[op.Key] + op.Val
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MergeShards{})
	labgob.Register(FetchShardsArgs{})
	labgob.Register(FetchShardsReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqm = make(map[int64]int64)
	kv.chm = make(map[int]chan interface{})
	kv.data = make(map[string]string)

	kv.durFetchShards = 0
	kv.shardsToCfgNum = make(map[int]int)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardsToCfgNum[i] = 0
	}

	kv.initStatus(persister)

	go kv.applyLoop()
	go kv.checkConfigLoop()

	return kv
}

/**FIXME: 在更新Config时，当同一个Group的其中一个Shard拉取失败后重启会导致再一次拉取所有Shards，
如重启之前这些已拉取的Shard被Append操作，会导致后面重启后拉取的数据覆盖
*/

func (kv *ShardKV) initStatus(persister *raft.Persister) {
	DPrintf("ShardKV %v me %v 开始恢复 \n", kv.gid, kv.me)
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
		DPrintf("Server %v lastSnapshotIndex %v lastapplied %v len(log) %v\n", kv.gid, lastSnapshotIndex, lastApplied, len(entries))
		for i := lastSnapshotIndex + 1; i <= lastApplied; i++ {
			op := entries[i-lastSnapshotIndex-1].Command.(Op)
			seq := kv.seqm[op.Cid]

			if op.Op == "MergeShards" {

				ms := op.UserData.(MergeShards)
				DPrintf("initStatus ShardKV %v Me %v MergeShards ms.Config %v Shards %v kv.config %v shardsToNum %v\n", kv.gid, kv.me, ms.Config, ms.Shards, kv.config, kv.shardsToCfgNum)
				if ms.Config.Num == kv.config.Num+1 {
					//防止提交了重复MergeShards，导致被覆盖
					if ms.Data != nil {
						repeat := false
						for i := 0; i < len(ms.Shards); i++ {
							if kv.shardsToCfgNum[ms.Shards[i]] == ms.Config.Num {
								repeat = true
							}
						}
						if repeat {
							continue
						}
					}

					//update shardsToCfgNum
					for i := 0; i < len(ms.Shards); i++ {
						kv.shardsToCfgNum[ms.Shards[i]] = ms.Config.Num
					}
					if ms.Data != nil {
						fetchShardsReply := ms.Data.(FetchShardsReply)
						//update data
						for k, v := range fetchShardsReply.Data {
							if vvv,ok:=kv.data[k];ok{
								DPrintf("ApplyLoop ShardKv %v me %v MergeShards Set Key %v OriVal %v NewVal %v\n",kv.gid,kv.me,k,vvv,v)
							}
							kv.data[k] = v
						}
						for cid, seq := range fetchShardsReply.Seqm {
							if sq, ok := kv.seqm[cid]; (!ok) || (seq > sq) {
								DPrintf("initStatus ShardKV %v Me %v Set the Seqm cid %v seq %v\n", kv.gid, kv.me, cid, seq)
								kv.seqm[cid] = seq
							}
						}
					}
					//update config
					pass := true
					for _, num := range kv.shardsToCfgNum {
						if num < ms.Config.Num {
							pass = false
						}
					}
					if pass {
						DPrintf("ShardKv %v Update Config %v\n", kv.gid, ms.Config)
						kv.config = ms.Config
					}
				}
				continue
			}

			if op.Op == "Get" || op.Op == "Put" || op.Op == "Append" {
				configNum := op.UserData.(int)
				shard := key2shard(op.Key)
				if kv.shardsToCfgNum[shard] != configNum {
					continue
				}
			}

			if op.Op != "MergeShardsReq" {
				if op.Seq > seq {
					DPrintf("InitStatus ShardKv %v Me %v set the seqm in applyloop OP %v Cid %v Seq %v Key %v Val %v\n", kv.gid, kv.me, op.Op, op.Cid, op.Seq, op.Key, op.Val)
					kv.seqm[op.Cid] = op.Seq
				} else if op.Op != "Get" {
					continue
				}
			}

			if op.Op == "Get" {
				continue
			} else if op.Op == "Put" {
				kv.Put(&op)
			} else if op.Op == "Append" {
				kv.Append(&op)
			} else if op.Op == "MergeShardsReq" {
				args := op.UserData.(FetchShardsArgs)
				for i := 0; i < len(args.Shards); i++ {
					if kv.shardsToCfgNum[args.Shards[i]] < args.NewConfig.Num {
						kv.shardsToCfgNum[args.Shards[i]] = args.NewConfig.Num
					}
				}
			}
		}
	}
	DPrintf("ShardKv %v Me %v ReStart Config %v Data %v\n", kv.gid, kv.me, kv.config, kv.data)
}

func (kv *ShardKV) applyLoop() {
	for {
		msg, ok := <-kv.applyCh
		if !ok {
			break
		}
		kv.mu.Lock()

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
		if op.Op == "Append" {
			DPrintf("ShardKv %v Me %v apply Index %v Cid %v Seq %v Key %v Val %v\n", kv.gid, kv.me, msg.CommandIndex, op.Cid, op.Seq, op.Key, op.Val)
		}

		//MergeShards如同Get请求一样，都不需要去重
		if op.Op == "MergeShards" {
			ms := op.UserData.(MergeShards)
			/**MergeShards 两种情况
			(1)shards不需要拉取数据，则直接提交op更新这些shards的config Num
			(2)shards需要拉取数据，则等待数据拉取完成
			*/
			//需要并发控制，如防止同一个Config拉取了两次
			if ms.Config.Num == kv.config.Num+1 {
				//检查是否重复
				if ms.Data != nil {
					repeat := false
					for i := 0; i < len(ms.Shards); i++ {
						if kv.shardsToCfgNum[ms.Shards[i]] == ms.Config.Num {
							DPrintf("kv.data %v ms.data %v\n",kv.data,ms.Data.(FetchShardsReply).Data)
							repeat = true
							break
						}
					}
					if repeat {
						atomic.StoreInt32(&kv.durFetchShards, 0)
						kv.mu.Unlock()
						continue
					}
				}

				//update shardsToCfgNum
				for i := 0; i < len(ms.Shards); i++ {
					kv.shardsToCfgNum[ms.Shards[i]] = ms.Config.Num
				}
				DPrintf("ShardKv %v Me %v ConfigNum %v 提交MergeShards Shards %v Now %v\n", kv.gid, kv.rf.Me(), ms.Config.Num, ms.Shards, kv.shardsToCfgNum)

				if ms.Data != nil {
					fetchShardsReply := ms.Data.(FetchShardsReply)
					//update data
					for k, v := range fetchShardsReply.Data {
						if vvv,ok:=kv.data[k];ok{
							DPrintf("ApplyLoop ShardKv %v me %v MergeShards Set Key %v OriVal %v NewVal %v\n",kv.gid,kv.me,k,vvv,v)
						}
						kv.data[k] = v
					}
					//update seqm
					/**如不更新seqm，可能出现当切片转移前，有一个Append操作被执行，但由于超时未返回给客户端，当切片转移后
					由于client的重试会发给新Group，而此时新Group的seq小于client发送的seq，导致本应被去重的seq请求被执行，
					最终造成重复Append
					*/
					for cid, seq := range fetchShardsReply.Seqm {
						if sq, ok := kv.seqm[cid]; (!ok) || (seq > sq) {
							DPrintf("applyLoop ShardKV %v Me %v Set the Seqm cid %v seq %v\n", kv.gid, kv.me, cid, seq)
							kv.seqm[cid] = seq
						}
					}
				}

				//update config
				pass := true
				for _, num := range kv.shardsToCfgNum {
					if num < ms.Config.Num {
						pass = false
					}
				}
				if pass {
					DPrintf("ShardKv %v Me %v Pass Cur Config %v Update Config %v And \n", kv.gid, kv.rf.Me(), kv.config, ms.Config)
					kv.config = ms.Config
					//取消设置并发标志
					atomic.StoreInt32(&kv.durFetchShards, 0)
				}
			} else {
				//可能出现ms.Config.Num == kv.config.Num
				atomic.StoreInt32(&kv.durFetchShards, 0)
			}

			kv.mu.Unlock()
			continue
		}
		//根据Config Num 过滤
		if op.Op == "Get" || op.Op == "Put" || op.Op == "Append" {
			configNum := op.UserData.(int)
			shard := key2shard(op.Key)
			if kv.shardsToCfgNum[shard] != configNum {
				DPrintf("ShardKv %v Config过滤 放弃 Index %v\n", kv.gid, msg.CommandIndex)
				//提交返回
				if _, iL := kv.rf.GetState(); iL {
					var reply interface{}
					switch op.Op {
					case "Get":
						reply = GetReply{
							Err: ErrWrongGroup,
						}
					case "Put":
						reply = PutAppendReply{
							Err: ErrWrongGroup,
						}
					case "Append":
						reply = PutAppendReply{
							Err: ErrWrongGroup,
						}
					}

					//唤醒对应协程
					ch, ok := kv.chm[msg.CommandIndex]
					if ok {
						delete(kv.chm, msg.CommandIndex)
						//接受到请求到Apply的间隔
						dur := (time.Now().UnixMilli() - op.InTime) * time.Hour.Milliseconds()
						if dur < int64(RequestTimeout-RequestTimeoutDeviation) {
							ch <- reply
						}
					}
				}
				kv.mu.Unlock()
				continue
			}
		}
		//根据Seq过滤
		if op.Op != "MergeShardsReq" {
			if op.Seq > seq {
				DPrintf("ShardKv %v Me %v set the seqm in applyloop OP %v Cid %v Seq %v Key %v Val %v\n", kv.gid, kv.me, op.Op, op.Cid, op.Seq, op.Key, op.Val)
				kv.seqm[op.Cid] = op.Seq
			} else if op.Op != "Get" {
				DPrintf("ShardKv %v Seq过滤 放弃 Index %v\n", kv.gid, msg.CommandIndex)

				//提前返回
				if _, iL := kv.rf.GetState(); iL {
					var reply interface{}
					switch op.Op {
					case "Put":
						reply = PutAppendReply{
							Err: ErrWrongGroup,
						}
					case "Append":
						reply = PutAppendReply{
							Err: ErrWrongGroup,
						}
					}
					//唤醒对应协程
					ch, ok := kv.chm[msg.CommandIndex]
					if ok {
						delete(kv.chm, msg.CommandIndex)
						//接受到请求到Apply的间隔
						dur := (time.Now().UnixMilli() - op.InTime) * time.Hour.Milliseconds()
						if dur < int64(RequestTimeout-RequestTimeoutDeviation) {
							ch <- reply
						}
					}
				}

				kv.mu.Unlock()
				continue
			}
		}

		if _, iL := kv.rf.GetState(); iL {
			//Leader
			var reply interface{}
			if op.Op == "Get" {
				reply = GetReply{
					Err:   OK,
					Value: kv.data[op.Key],
				}
				DPrintf("ShardKv %v Leader %v exec Index %v Seq %v Get key %v\n", kv.gid, kv.rf.Me(), msg.CommandIndex, op.Seq, op.Key)
			} else {
				//必须保证PutAppend和seq更新在同一处更新，否则会出现以下丢失log情况
				//错误过程，由于请求有超时时间，如收到apply后更新seq，但在请求协程中处理PutAppend可能会出现seq更新了，但实际上该seq操作（PutAppend）并未执行
				if op.Op == "Put" {
					kv.Put(&op)
					reply = PutAppendReply{
						Err: OK,
					}
					DPrintf("ShardKv %v Leader %v exec Index %v Seq %v PutApeend key %v\n", kv.gid, kv.rf.Me(), msg.CommandIndex, op.Seq, op.Key)
				} else if op.Op == "Append" {
					kv.Append(&op)
					reply = PutAppendReply{
						Err: OK,
					}
					DPrintf("ShardKv %v Leader %v exec Index %v Seq %v PutApeend key %v\n", kv.gid, kv.rf.Me(), msg.CommandIndex, op.Seq, op.Key)
				} else if op.Op == "MergeShardsReq" {
					kvs := make(map[string]string, 0)
					args := op.UserData.(FetchShardsArgs)
					//为shards构造map
					sm := make(map[int]bool, 0)
					for i := 0; i < len(args.Shards); i++ {
						if kv.shardsToCfgNum[args.Shards[i]] < args.NewConfig.Num {
							kv.shardsToCfgNum[args.Shards[i]] = args.NewConfig.Num
							DPrintf("ShardKv %v Leader %v Shard %v be set Num %v\n",kv.gid,kv.me,args.Shards[i],args.NewConfig.Num)
						}
						sm[args.Shards[i]] = true
					}
					//因为多次请求，所以不应删除任何key
					for k, v := range kv.data {
						if _, ok := sm[key2shard(k)]; ok {
							kvs[k] = v
						}
					}
					seqm := make(map[int64]int64, 0)
					for k, v := range kv.seqm {
						seqm[k] = v
					}
					reply = FetchShardsReply{
						Err:  OK,
						Data: kvs,
						Seqm: seqm,
					}
				}
			}
			//唤醒对应协程
			ch, ok := kv.chm[msg.CommandIndex]
			if ok {
				delete(kv.chm, msg.CommandIndex)
				//接受到请求到Apply的间隔
				dur := (time.Now().UnixMilli() - op.InTime) * time.Hour.Milliseconds()
				if dur < int64(RequestTimeout-RequestTimeoutDeviation) {
					ch <- reply
				}
			}
		} else {
			//Follower
			if op.Op != "Get" {
				// DPrintf("ShardKv %v Follower %v exec Index %v Seq %v PutApeend key %v\n", kv.gid, kv.rf.Me(), msg.CommandIndex, op.Seq, op.Key)
				if op.Op == "Put" {
					kv.Put(&op)
				} else if op.Op == "Append" {
					kv.Append(&op)
				} else if op.Op == "MergeShardsReq" {
					args := op.UserData.(FetchShardsArgs)
					for i := 0; i < len(args.Shards); i++ {
						if kv.shardsToCfgNum[args.Shards[i]] < args.NewConfig.Num {
							kv.shardsToCfgNum[args.Shards[i]] = args.NewConfig.Num
						}
					}
				}
			}
		}

		//保存快照
		if kv.maxraftstate < kv.rf.GetRaftStateSize() && kv.maxraftstate > 0 {
			w := bytes.NewBuffer([]byte{})
			e := labgob.NewEncoder(w)
			e.Encode(kv.seqm)
			e.Encode(kv.data)
			e.Encode(kv.shardsToCfgNum)
			e.Encode(kv.config)
			kv.rf.Snapshot(kv.lastApply, w.Bytes())
		}

		kv.mu.Unlock()
	}
	DPrintf("ShardKv %v quit applyLoop\n", kv.gid)
}

func (kv *ShardKV) applySnapshot(data []byte) {
	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	seqm := make(map[int64]int64)
	kvdata := make(map[string]string)
	shardsToCfgNum := make(map[int]int)
	config := shardctrler.Config{}
	d.Decode(&seqm)
	d.Decode(&kvdata)
	d.Decode(&shardsToCfgNum)
	d.Decode(&config)
	kv.seqm = seqm
	kv.data = kvdata
	kv.shardsToCfgNum = shardsToCfgNum
	kv.config = config
}

func (kv *ShardKV) checkConfigLoop() {
	for !kv.killed() {
		time.Sleep(CheckConfigTimeout)

		if _, iL := kv.rf.GetState(); iL && (atomic.LoadInt32(&kv.durFetchShards) == 0) {
			kv.mu.Lock()
			oldConfig := kv.config
			kv.mu.Unlock()

			newConfig := kv.mck.Query(oldConfig.Num + 1)

			if newConfig.Num == 0 || newConfig.Num != oldConfig.Num+1 {
				continue
			}

			//设置并发标志
			atomic.StoreInt32(&kv.durFetchShards, 1)
			DPrintf("ShardKv %v checkConfigLoop oldConfig %v newConfig %v\n", kv.gid, oldConfig, newConfig)

			shardsMap := make(map[int]bool, 0)
			for i := 0; i < len(newConfig.Shards); i++ {
				shardsMap[i] = true
			}

			if newConfig.Num == 1 && oldConfig.Num == 0 {
				shards := make([]int, 0)
				for k, _ := range shardsMap {
					shards = append(shards, k)
				}
				op := Op{
					Op: "MergeShards",
					UserData: MergeShards{
						Gid:    -1,
						Config: newConfig,
						Shards: shards,
						Data:   nil,
					},
				}
				kv.rf.Start(op)
				continue
			}

			gidToshards := kv.checkShards(oldConfig, newConfig)
			if len(gidToshards) > 0 {
				for gid, shards := range gidToshards {
					//移除需要拉取数据的shards
					DPrintf("ShardKv %v 向 ShardKv %v 拉取 Shards %v 的数据\n", kv.gid, gid, shards)
					for i := 0; i < len(shards); i++ {
						if _, ok := shardsMap[shards[i]]; ok {
							delete(shardsMap, shards[i])
						}
					}
					//开始拉取数据
					go kv.startFetchShards(oldConfig, newConfig, gid, shards)
				}
			}
			DPrintf("ShardKv %v 来到了这 checkConfigLoop oldConfig %v newConfig %v\n", kv.gid, oldConfig, newConfig)
			//先提交一个
			shards := make([]int, 0)
			for k, _ := range shardsMap {
				shards = append(shards, k)
			}
			op := Op{
				Op: "MergeShards",
				UserData: MergeShards{
					Gid:    -1,
					Config: newConfig,
					Shards: shards,
					Data:   nil,
				},
			}
			index, _, isLeader := kv.rf.Start(op)
			if isLeader {
				DPrintf("ShardKv %v Index %v 提交MergeShards \n", kv.gid, index)
			}
		}

		DPrintf("ShardKv %v me %v durFetchShards %v\n", kv.gid, kv.me, atomic.LoadInt32(&kv.durFetchShards))
	}
}

func (kv *ShardKV) checkShards(oldConfig shardctrler.Config, newConfig shardctrler.Config) map[int][]int {
	//gid->gidToshards
	gidToshards := make(map[int][]int, 0)
	for i := 0; i < len(newConfig.Shards); i++ {
		if newConfig.Shards[i] != oldConfig.Shards[i] && newConfig.Shards[i] == kv.gid {
			gidToshards[oldConfig.Shards[i]] = append(gidToshards[oldConfig.Shards[i]], i)
		}
	}
	return gidToshards
}

func (kv *ShardKV) startFetchShards(oldConfig shardctrler.Config, newConfig shardctrler.Config, gid int, shards []int) {
	args := FetchShardsArgs{
		Op:        "MergeShardsReq",
		Config:    oldConfig,
		NewConfig: newConfig,
		Shards:    shards,
	}

	for {
		if _, iL := kv.rf.GetState(); !iL {
			return
		}
		if servers, ok := oldConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply FetchShardsReply
				ok := kv.sendFetchShards(srv, &args, &reply)
				if ok && (reply.Err == OK) {
					//add shards
					// DPrintf("ShardKv Gid %v Me %v startFetchShards 发送的MergeShards durFetchShards %v reply data %v\n",kv.gid,kv.me,atomic.LoadInt32(&kv.durFetchShards),reply.Data)
					op := Op{
						Op: "MergeShards",
						UserData: MergeShards{
							Gid:    gid,
							Config: newConfig,
							Shards: shards,
							Data:   reply,
						},
					}
					kv.rf.Start(op)
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(RequestTimeout)
	}
}

func (kv *ShardKV) sendFetchShards(srv *labrpc.ClientEnd, args *FetchShardsArgs, reply *FetchShardsReply) bool {
	return srv.Call("ShardKV.FetchShards", args, reply)
}

func (kv *ShardKV) FetchShards(args *FetchShardsArgs, reply *FetchShardsReply) {
	kv.mu.Lock()
	if kv.config.Num < args.Config.Num {
		DPrintf("ShardKv %v RecvFetchShards kv.config.num %v args.config.num %v\n", kv.gid, kv.config.Num, args.Config.Num)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	op := Op{
		Op:       args.Op,
		InTime:   time.Now().UnixMilli(),
		UserData: *args,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("ShardKV %v recv Index %v FetchShards Shards %v\n", kv.gid, index, args.Shards)
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
		rp := c.(FetchShardsReply)
		reply.Err = rp.Err
		reply.Data = rp.Data
		reply.Seqm = rp.Seqm
		DPrintf("ShardKv %v reply FetchShards args.Shards %v Data %v\n", kv.gid, args.Shards, reply.Data)
		return
	}
}
