package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	OpType int32
	Key    string
	Value  string

	ClientId int64
	MsgId    int64
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	closeCh chan struct{}

	maxraftstate int // snapshot if log grows this big

	seqLock sync.RWMutex
	seqMap  map[int64]int64

	data map[string]string // 存储数据的地方

	historyLock sync.RWMutex
	history     map[int64]string

	notifyLock  sync.RWMutex
	notifyChMap map[int]chan Op

	persister     *raft.Persister
	snapshotIndex int
}

func (kv *KVServer) getSeq(clientId int64) int64 {
	return kv.seqMap[clientId]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 如果 kvserver 不是多数派的一部分，就不应完成 Get RPC
	// 以避免服务过时数据
	// 目前采用的简单方法是每个Get命令也记录在raft日志中
	select {
	case <-kv.closeCh:
		return
	default:
	}

	//fmt.Printf("[%d]接收到请求,args=%+v\n", kv.me, args)

	// 幂等检查
	kv.seqLock.RLock()
	if seq := kv.seqMap[args.ClientId]; args.MsgId <= seq {
		//fmt.Printf("请求被幂等性过滤,seq:%+v,当前seq:%d\n", args, kv.seqMap[args.ClientId])
		kv.seqLock.RUnlock()
		reply.Err = OK
		if args.MsgId == seq {
			kv.historyLock.RLock()
			reply.Value = kv.history[args.ClientId]
			kv.historyLock.RUnlock()
		}
		return
	}
	kv.seqLock.RUnlock()

	// 检查当前节点是否是leader,并提交
	op := Op{
		OpType:   GET,
		Key:      args.Key,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		// 可以在OP上附加term,也可以在message上附加term
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		//EPrintf("[%d] 请求被非leader节点过滤,req=%+v", kv.me, args)
		reply.Err = ErrWrongLeader
		return
	}

	//EPrintf("[%d] leader接收到Get请求,req=%+v", kv.me, args)

	timeout := time.After(500 * time.Millisecond)

	// 准备监听
	notifyCh := make(chan Op, 1)
	kv.notifyLock.Lock()
	kv.notifyChMap[index] = notifyCh
	kv.notifyLock.Unlock()
	defer func() {
		kv.notifyLock.Lock()
		delete(kv.notifyChMap, index)
		kv.notifyLock.Unlock()
	}()

	// 监听
	select {
	case <-kv.closeCh:
		reply.Err = ErrWrongLeader
	case op := <-notifyCh:
		reply.Value = op.Value
		reply.Err = OK
		//DPrintf("[%d] 读请求已应用,GET,index=%d,key=%s,value=%s", kv.me, index, op.Key, reply.Value)
	case <-timeout:
		reply.Err = ErrTimeout
		//fmt.Printf("[%d] 请求过期\n", kv.me)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	select {
	case <-kv.closeCh:
		return
	default:
	}

	//fmt.Printf("[%d]接收到请求,args=%+v\n", kv.me, args)

	// 检查是否是过去的请求
	kv.seqLock.RLock()
	if args.MsgId <= kv.getSeq(args.ClientId) {
		//fmt.Printf("请求被幂等性过滤,seq:%+v,当前seq:%d\n", args, kv.seqMap[args.ClientId])
		kv.seqLock.RUnlock()
		reply.Err = OK
		return
	}
	kv.seqLock.RUnlock()

	//EPrintf("[%d] leader接收到PutAppend请求,req=%+v", kv.me, args)

	op := Op{
		OpType: args.OpType,
		Key:    args.Key,
		Value:  args.Value,

		ClientId: args.ClientId,
		MsgId:    args.MsgId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		//EPrintf("[%d] 请求被非leader节点过滤,req=%+v", kv.me, args)
		reply.Err = ErrWrongLeader
		return
	}

	//EPrintf("[%d] leader接收到PutAppend请求,req=%+v", kv.me, args)

	timeout := time.After(500 * time.Millisecond)

	notifyCh := make(chan Op, 1)
	kv.notifyLock.Lock()
	kv.notifyChMap[index] = notifyCh
	kv.notifyLock.Unlock()
	defer func() {
		kv.notifyLock.Lock()
		delete(kv.notifyChMap, index)
		kv.notifyLock.Unlock()
	}()

	select {
	case <-kv.closeCh:
		reply.Err = ErrWrongLeader
	case <-notifyCh:
		reply.Err = OK
	case <-timeout:
		reply.Err = ErrTimeout
		//fmt.Printf("[%d] 请求过期\n", kv.me)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.closeCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[]: 一组 kvserver 的 RPC 通道端点
// me: 当前服务器在 servers[] 中的索引
// maxraftstate：Raft状态机允许的最大持久化大小（字节数）
// 如果 maxraftstate == -1，就表示 不需要 snapshot
// StartKVServer 函数必须立刻返回
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raft.ApplyMsg),
		closeCh:      make(chan struct{}),

		seqMap:      map[int64]int64{},
		notifyChMap: map[int]chan Op{},
		data:        map[string]string{},
		history:     map[int64]string{},

		persister: persister,
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readPersist(persister.ReadSnapshot())

	go kv.forwardMsg()

	return kv
}
