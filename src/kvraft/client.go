package kvraft

import (
	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd // 与服务器通信的handler

	clientId int64 // Clerk的Id
	msgId    int64 // 消息序列号
	leaderId int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:  servers,
		clientId: nrand(),
		msgId:    0,
		leaderId: int(nrand()) % len(servers),
	}
	return ck
}

// 获取键的当前值
// 如果键不存在，则返回""
// 在遇到其他所有错误时，会无限次尝试
func (ck *Clerk) Get(key string) string {
	ck.msgId++

	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		MsgId:    ck.msgId,
	}

	failedTimes := 0

	for {
		var reply GetReply
		EPrintf("[%d] clerk发送了Get请求,req=%+v", ck.clientId, args)
		if ok := ck.callGet(ck.leaderId, &args, &reply); ok {
			switch reply.Err {
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case OK:
				//EPrintf("Get请求完成,clientid=%d,msgid=%d", ck.clientId, ck.msgId)
				return reply.Value
			case ErrNoKey:
				//EPrintf("Get请求完成,clientid=%d,msgid=%d", ck.clientId, ck.msgId)
				return ""
			case ErrTimeout:
				failedTimes++
				if failedTimes >= 3 {
					// 如果超时次数三次的话,认定这是个假leader
					ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
					failedTimes = 0
				}
			default:
				panic(reply.Err) // debug
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) PutAppend(key string, value string, opType int32) {
	ck.msgId++

	args := PutAppendArgs{
		OpType:   opType,
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		MsgId:    ck.msgId,
	}

	failedTimes := 0

	for {
		var reply PutAppendReply
		EPrintf("[%d] clerk发送了PutAppend请求,req=%+v", ck.clientId, args)
		if ok := ck.callPutAppend(ck.leaderId, &args, &reply); ok {
			switch reply.Err {
			case OK:
				//EPrintf("PutAppend请求完成,clientid=%d,msgid=%d", ck.clientId, ck.msgId)
				return
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case ErrTimeout:
				failedTimes++
				if failedTimes >= 3 {
					// 如果超时次数三次的话,认定这是个假leader
					ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
					failedTimes = 0
				}
			default:
				panic(reply.Err) // debug
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) callGet(server int, args *GetArgs, reply *GetReply) bool {
	return ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) callPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	return ck.servers[server].Call("KVServer.PutAppend", args, reply)
}

// func (ck *Clerk) callPut(server int, args *PutArgs, reply *PutReply) bool {
// 	return ck.servers[server].Call("KVServer.Put", args, reply)
// }

// func (ck *Clerk) calAppend(server int, args *AppendArgs, reply *AppendReply) bool {
// 	return ck.servers[server].Call("KVServer.Append", args, reply)
// }
