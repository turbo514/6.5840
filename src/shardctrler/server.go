package shardctrler

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const ExecuteTimeout = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	closeCh chan struct{}

	configs []Config // indexed by config num

	notifyChMap map[int]chan Op
	seqMap      map[int64]int64

	history     map[int64]*Config
	historyLock sync.RWMutex
}

type Op struct {
	OpType int32

	ClientId  int64
	RequestId int64

	Servers map[int][]string // Join, new GID -> servers

	GIDs []int // Leave

	Shard int // Move
	GID   int // Move

	Num       int    // Query
	Configure Config // Query
}

// Join 用于添加新副本组
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	select {
	case <-sc.closeCh:
		return
	default:
	}

	// 幂等检查 // 太多重复
	sc.mu.Lock()
	if sc.isInvalidRequest(args.ClientId, args.RequestId) {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()

	op := Op{
		OpType:    JOIN,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Servers:   args.Servers,
	}
	notifyCh, index, isLeader := sc.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	defer func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		delete(sc.notifyChMap, index)
	}()

	timeout := time.After(ExecuteTimeout)

	select {
	case <-sc.closeCh:
		reply.Err = ErrClosed
		reply.WrongLeader = true
	case <-notifyCh:
		reply.Err = OK
	case <-timeout:
		reply.Err = ErrTimeout
		reply.WrongLeader = true
	}
}

// Leave 用于移除现有的副本组
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	select {
	case <-sc.closeCh:
		return
	default:
	}

	// 幂等检查
	sc.mu.Lock()
	if sc.isInvalidRequest(args.ClientId, args.RequestId) {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()

	op := Op{
		OpType:    LEAVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		GIDs:      args.GIDs,
	}
	notifyCh, index, isLeader := sc.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	defer func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		delete(sc.notifyChMap, index)
	}()

	timeout := time.After(ExecuteTimeout)

	select {
	case <-sc.closeCh:
		reply.Err = ErrClosed
		reply.WrongLeader = true
	case <-notifyCh:
		reply.Err = OK
	case <-timeout:
		reply.Err = ErrTimeout
		reply.WrongLeader = true
	}
}

// Move 用于将某个分片移动给某个组
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	select {
	case <-sc.closeCh:
		return
	default:
	}

	// 幂等检查
	sc.mu.Lock()
	if sc.isInvalidRequest(args.ClientId, args.RequestId) {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()

	op := Op{
		OpType:    MOVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     args.Shard,
		GID:       args.GID,
	}
	notifyCh, index, isLeader := sc.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	defer func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		delete(sc.notifyChMap, index)
	}()

	timeout := time.After(ExecuteTimeout)

	select {
	case <-sc.closeCh:
		reply.Err = ErrClosed
		reply.WrongLeader = true
		return
	case <-notifyCh:
		reply.Err = OK
	case <-timeout:
		reply.Err = ErrTimeout
		reply.WrongLeader = true
	}
}

// Query 用于查询配置信息,若 Num == -1 或大于已知的最大配置号, ShardCtrler 应回复最新配置
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	select {
	case <-sc.closeCh:
		return
	default:
	}

	// 幂等检查
	sc.mu.Lock()
	if sc.isInvalidRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		reply.Config = *sc.history[args.ClientId]
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		OpType:    QUERY,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Num:       args.Num,
	}
	notifyCh, index, isLeader := sc.start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	defer func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		delete(sc.notifyChMap, index)
	}()

	timeout := time.After(ExecuteTimeout)

	select {
	case <-sc.closeCh:
		reply.Err = ErrClosed
		reply.WrongLeader = true
	case op := <-notifyCh:
		reply.Err = OK
		reply.Config = op.Configure
	case <-timeout:
		reply.Err = ErrTimeout
		reply.WrongLeader = true // TODO: 消除这个
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	close(sc.closeCh)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := &ShardCtrler{
		me:          me,
		closeCh:     make(chan struct{}),
		configs:     make([]Config, 1),
		applyCh:     make(chan raft.ApplyMsg),
		notifyChMap: make(map[int]chan Op),
		seqMap:      make(map[int64]int64),
		history:     map[int64]*Config{},
	}
	labgob.Register(Op{})

	sc.configs[0].Groups = map[int][]string{}
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	go sc.forwardMsg()

	return sc
}

func (sc *ShardCtrler) isInvalidRequest(clientId int64, requestId int64) bool {
	if seqId := sc.seqMap[clientId]; requestId <= seqId {
		return true
	}
	return false
}

// 并发安全,外面别加锁
func (sc *ShardCtrler) start(op Op) (<-chan Op, int, bool) {
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return nil, 0, false
	}

	notifyCh := make(chan Op, 1)

	sc.mu.Lock()
	sc.notifyChMap[index] = notifyCh
	sc.mu.Unlock()

	//DPrintf("[%d] 接收到命令,op=%+v", sc.me, op)
	return notifyCh, index, true
}
