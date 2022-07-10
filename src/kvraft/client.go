package kvraft

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cid       int64
	seq       int64
	curLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.cid = buildCid()
	ck.seq = 0

	return ck
}

func buildCid() int64 {
	cid := make([]byte, 8)
	_, err := io.ReadFull(rand.Reader, cid)
	if err != nil {
		panic(err)
	}
	return int64(binary.BigEndian.Uint64(cid))
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	atomic.AddInt64(&ck.seq, 1)
	args := &GetArgs{
		Key: key,
		Cid: ck.cid,
		Seq: ck.seq,
	}

	for{
		{
			reply := &GetReply{}
			ok:=ck.servers[ck.curLeader].Call("KVServer.Get", args, reply)
			if ok {
				if reply.Error()!=ErrWrongLeader{
					return reply.Value
				}	
			}
		}
	
		for i := 0; i < len(ck.servers); i++ {
			if i==ck.curLeader {
				continue
			}
			reply := &GetReply{}
			ok:=ck.servers[i].Call("KVServer.Get", args, reply)
			if ok {
				if reply.Error()!=ErrWrongLeader {
					ck.curLeader=i
					return reply.Value
				}
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	atomic.AddInt64(&ck.seq, 1)
	args:=&PutAppendArgs{
		Cid: ck.cid,
		Seq: ck.seq,
		Key: key,
		Value: value,
		Op: op,
	}

	for{
		{
			reply:=&PutAppendReply{}
			ok:=ck.servers[ck.curLeader].Call("KVServer.PutAppend",args,reply)
			if ok {
				if reply.Error()!=ErrWrongLeader {
					return
				}
			}
		}

		for i := 0; i < len(ck.servers); i++ {
			if i==ck.curLeader {
				continue
			}
			reply:=&PutAppendReply{}
			ok:=ck.servers[i].Call("KVServer.PutAppend",args,reply)
			if ok {
				if reply.Error()!=ErrWrongLeader {
					ck.curLeader=i
					return
				}
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
