package shardctrler

//
// Shardctrler clerk.
//

import (
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	ClientId  int64
	RequestId int64
	LeaderId  int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:   servers,
		ClientId:  nrand(),
		RequestId: 0,
		LeaderId:  int(nrand()) % len(servers),
	}
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.RequestId++
	args := &QueryArgs{
		Num: num,
		DeduplicateArgs: DeduplicateArgs{
			ClientId:  ck.ClientId,
			RequestId: ck.RequestId,
		},
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Join 管理员用于添加新副本组，参数是从唯一、非零副本组标识符到服务器名称列表的映射集
func (ck *Clerk) Join(servers map[int][]string) {
	ck.RequestId++
	args := &JoinArgs{
		Servers: servers,
		DeduplicateArgs: DeduplicateArgs{
			ClientId:  ck.ClientId,
			RequestId: ck.RequestId,
		},
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.RequestId++
	args := &LeaveArgs{
		GIDs: gids,
		DeduplicateArgs: DeduplicateArgs{
			ClientId:  ck.ClientId,
			RequestId: ck.RequestId,
		},
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 分片控制器创建新配置将分片分配给指定组，主要用于测试
func (ck *Clerk) Move(shard int, gid int) {
	ck.RequestId++
	args := &MoveArgs{
		Shard: shard,
		GID:   gid,
		DeduplicateArgs: DeduplicateArgs{
			ClientId:  ck.ClientId,
			RequestId: ck.RequestId,
		},
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
