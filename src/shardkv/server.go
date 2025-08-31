package shardkv

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	//ctrlers  []*labrpc.ClientEnd
	persister *raft.Persister
	clerk     *shardctrler.Clerk

	closeCh chan struct{}

	shards        map[int]*Shard
	snapshotIndex int
	maxraftstate  int // snapshot if log grows this big

	seqMap map[int64]int64 // clientId -> requestId
	//history     map[int64]string           // clientId -> value
	notifyChMap map[int]chan CommandResult // index -> channel

	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config

	pullNotifier chan struct{}
	gcNotifier   chan struct{}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// 检查是否由自己负责
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if !kv.checkShardAndState(shardId) {
		// DPrintf([%d:%d]不负责分片[%s]", kv.me, kv.gid, args.Key)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// if kv.isInvalidRequest(args.ClientId, args.RequestId) {
	// 	reply.Err = OK
	// 	reply.Value = kv.history[args.ClientId] // TODO: 可以改成太过时的请求不填充
	// 	kv.mu.Unlock()
	// 	return
	// }

	if args.RequestId < kv.seqMap[args.ClientId] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	// 提交命令和等待应用
	cmd := CommandArgs{
		CommandType: GET,
		Data:        *args,
	}
	var result CommandResult
	kv.StartCommand(cmd, &result, 500*time.Millisecond)
	//DPrintf("[%d:%d] result=%+v", kv.me, kv.gid, result)
	if result.Err == OK {
		reply.Err, reply.Value = OK, result.Value.(string)
		//DPrintf("[%d:%d] 返回Get响应,key=%s,value=%s", kv.me, kv.gid, args.Key, reply.Value)
	} else {
		reply.Err = result.Err
		//DPrintf("[%d:%d] 返回Get响应,key=%s", kv.me, kv.gid, args.Key)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 检查是否由自己负责
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if !kv.checkShardAndState(shardId) {
		// DPrintf([%d:%d]不负责分片[%s]", kv.me, kv.gid, args.Key)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 幂等检查,过滤部分过时请求
	if kv.isInvalidRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	cmd := CommandArgs{
		CommandType: args.Op,
		Data:        *args,
	}
	var result CommandResult
	kv.StartCommand(cmd, &result, 500*time.Millisecond)

	reply.Err = result.Err
}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
	kv.mu.Lock()
	// 判断是否是合法的请求
	if args.ConfigNum != kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := CommandArgs{
		CommandType: GETSHARD,
		Data:        *args,
	}
	var result CommandResult
	kv.StartCommand(cmd, &result, 500*time.Millisecond)

	if result.Err != OK {
		reply.Err = result.Err
	} else {
		res := result.Value.(GetShardsReply)
		reply.Err = OK
		reply.ConfigNum = res.ConfigNum
		reply.SeqMap = res.SeqMap
		reply.Shards = res.Shards
	}
}

func (kv *ShardKV) RemoveShards(args *RemoveShardsArgs, reply *RemoveShardsReply) {
	cmd := CommandArgs{
		CommandType: REMOVESHARD,
		Data:        *args,
	}
	result := CommandResult{}
	kv.StartCommand(cmd, &result, deleteShardMaxWait)
	reply.Err = result.Err
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()

	close(kv.closeCh)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(CommandArgs{})
	labgob.Register(CommandResult{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(GetShardsReply{})
	labgob.Register(RestoreShardsArgs{})
	labgob.Register(RemoveShardsArgs{})
	labgob.Register(Shard{})
	labgob.Register(SnapshotStatus{})
	labgob.Register(GetShardsArgs{})
	labgob.Register(GetShardsReply{})

	kv := &ShardKV{
		me:       me,
		make_end: make_end,
		gid:      gid,
		//ctrlers:  ctrlers,
		applyCh:   make(chan raft.ApplyMsg),
		clerk:     shardctrler.MakeClerk(ctrlers),
		persister: persister,
		closeCh:   make(chan struct{}),

		shards:        make(map[int]*Shard),
		snapshotIndex: 0,
		maxraftstate:  maxraftstate,

		seqMap: map[int64]int64{},
		//history:     map[int64]string{},
		notifyChMap: map[int]chan CommandResult{},

		lastConfig:    shardctrler.Config{},
		currentConfig: shardctrler.Config{},

		pullNotifier: make(chan struct{}, 1),
		gcNotifier:   make(chan struct{}, 1),
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.initShards()

	kv.readPersist(persister.ReadSnapshot())

	go kv.forawrdMsg()
	go kv.monitorUpdateConfigFunc()
	go kv.monitorPullFunc()
	go kv.monitorGCFunc()

	return kv
}
func (kv *ShardKV) initShards() {
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		if _, ok := kv.shards[shardId]; !ok {
			kv.shards[shardId] = makeShard(SERVING)
		}
	}
}
