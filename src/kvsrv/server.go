package kvsrv

import (
	"log"
	"sync"
)

var Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.RWMutex

	// Your definitions here.
	data    map[string]string
	seq     map[int64]int64
	history map[int64]string
}

func (kv *KVServer) getSeq(clientId int64) int64 {
	return kv.seq[clientId]
}

func (kv *KVServer) setSeq(clientId int64, seq int64) {
	kv.seq[clientId] = seq
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	reply.Value = kv.data[key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	key := args.Key
	value := args.Value
	clientId := args.ClientId
	seq := args.MsgId

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否是新请求
	if seq <= kv.getSeq(clientId) {
		// 不是新请求
		return
	}

	// 对于新请求,进行操作
	kv.data[key] = value

	// 进行缓存
	kv.setSeq(clientId, seq)

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	key := args.Key
	value := args.Value
	clientId := args.ClientId
	seq := args.MsgId

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否新请求
	if seq <= kv.getSeq(clientId) {
		// 不是新请求,返回缓存的value
		reply.Value = kv.history[clientId]
		return
	}

	// 对于新请求,执行实际操作
	old := kv.data[key]
	kv.data[key] = old + value

	reply.Value = old

	// 缓存
	kv.setSeq(clientId, seq)
	kv.history[clientId] = old
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data:    make(map[string]string),
		seq:     make(map[int64]int64),
		history: make(map[int64]string),
	}
	// You may need initialization code here.

	return kv
}
