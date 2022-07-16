package shardkv

import (
	"time"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)
const RequestTimeout = time.Millisecond * 500
const RequestTimeoutDeviation = time.Millisecond * 50
const CheckConfigTimeout=time.Millisecond*100

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid int64
	Seq int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64
	Seq int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type FetchShardsArgs struct{
	Op string
	Cid int64
	Seq int64
	Shards []int
	Config shardctrler.Config
	NewConfig shardctrler.Config
}

type FetchShardsReply struct{
	Err Err
	Data map[string]string
	Seqm map[int64]int64
}

type MergeShards struct{
	Gid int
	Config shardctrler.Config
	Shards []int
	Data interface{}
}