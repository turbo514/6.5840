package kvsrv

import (
	"crypto/rand"
	"math/big"
	random "math/rand"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd

	clientId int64
	msgId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		server:   server,
		clientId: nrand(),
		msgId:    1,
	}

	return ck
}

func (ck *Clerk) Get(key string) string {
	var ret string
	for {
		reply, ok := ck.CallGet(&GetArgs{
			Key: key,
		})

		if ok {
			ret = reply.Value
			break
		}

		time.Sleep(time.Millisecond * time.Duration(100+random.Intn(150)))
	}
	return ret
}

func (ck *Clerk) CallGet(arg *GetArgs) (*GetReply, bool) {
	var reply GetReply
	if ck.server.Call("KVServer.Get", arg, &reply) {
		return &reply, true
	} else {
		return &reply, false
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	msgId := atomic.AddInt64(&ck.msgId, 1)

	var ret string

	for {
		reply, ok := ck.CallPutAppend(
			&PutAppendArgs{
				Key:      key,
				Value:    value,
				ClientId: ck.clientId,
				MsgId:    msgId,
			},
			op,
		)

		if ok {
			ret = reply.Value
			break
		}

		time.Sleep(time.Millisecond * time.Duration(100+random.Intn(150)))
	}

	return ret
}

func (ck *Clerk) CallPutAppend(arg *PutAppendArgs, op string) (*PutAppendReply, bool) {
	var reply PutAppendReply
	if ck.server.Call("KVServer."+op, arg, &reply) {
		return &reply, true
	} else {
		return &reply, false
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
