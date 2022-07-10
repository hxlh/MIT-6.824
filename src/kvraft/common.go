package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const RequestTimeout = time.Millisecond*500
const RequestTimeoutDeviation = time.Millisecond*50

type Err string

type ErrInterface interface {
	Error() string
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid int64
	Seq int64
}

type PutAppendReply struct {
	Err Err
}

func (p *PutAppendReply) Error() string {
	return string(p.Err)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64
	Seq int64
}

type GetReply struct {
	Err   Err
	Value string
}

func (g *GetReply) Error() string {
	return string(g.Err)
}
