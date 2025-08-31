package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"fmt"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

type Clerk struct {
	sm       *shardctrler.Clerk // shardctrler
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	clientId  int64
	requestId int64
}

// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		clientId:  nrand(),
		requestId: 0,
	}
	ck.config = ck.sm.Query(-1) // 获取最新配置

	return ck
}

// Get 获取键的当前值
// 若键不存在则返回""
// 遇到其他所有错误时将无限期尝试
func (ck *Clerk) Get(key string) string {
	ck.requestId++
	args := GetArgs{
		Key: key,
		DeduplicateArgs: DeduplicateArgs{
			ClientId:  ck.clientId,
			RequestId: ck.requestId,
		},
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ { // FIXME: 或许可以记住leaderId?
				srv := ck.make_end(servers[si])
				var reply GetReply
				if ok := srv.Call("ShardKV.Get", &args, &reply); ok {
					switch reply.Err {
					case ErrNoKey:
						return ""
					case OK:
						return reply.Value
					case ErrWrongGroup:
						DPrintf("[%d] 发送请求至错误的消息组", ck.clientId)
						goto for_end
					case ErrWrongLeader:
					case ErrTimeout:
					case ErrClosed:
					default:
						panic(fmt.Sprintf("出现不知名错误:%s", reply.Err))
					}
				}
			}
		for_end:
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op int32) {
	ck.requestId++
	args := PutAppendArgs{
		Op:    op,
		Key:   key,
		Value: value,
		DeduplicateArgs: DeduplicateArgs{
			ClientId:  ck.clientId,
			RequestId: ck.requestId,
		},
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok { // FIXME: 或许可以记住leaderId?
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				if ok := srv.Call("ShardKV.PutAppend", &args, &reply); ok {
					switch reply.Err {
					case OK:
						return
					case ErrWrongGroup:
						//DPrintf("[%d] 发送请求至错误的消息组", ck.clientId)
						goto for_end
					case ErrWrongLeader:
					case ErrTimeout:
					case ErrClosed:
					default:
						panic(fmt.Sprintf("出现不知名错误:%s", reply.Err))
					}
				}
			}
		for_end:
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
