package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrClosed      = "ErrClosed"
	ErrDuplicate   = "ErrDuplicate"

	ErrOther = "ErrOther"
)

type Err string

const (
	GET          int32 = 1
	PUT          int32 = 2
	APPEND       int32 = 3
	ADDCONFIG    int32 = 4
	ADDSHARD     int32 = 5
	REMOVESHARD  int32 = 6
	RESTORESHARD int32 = 7
	GETSHARD     int32 = 8
)

type DeduplicateArgs struct {
	ClientId  int64
	RequestId int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    int32

	DeduplicateArgs
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	DeduplicateArgs
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardsArgs struct {
	ConfigNum int
	Gid       int
	ShardIds  []int
}

type GetShardsReply struct {
	ConfigNum int
	Shards    map[int]Shard   // 每个分片号对应的完整分片
	SeqMap    map[int64]int64 // 每个分片对应的最近一次的请求Id
	//History   map[int64]string
	Err Err
}

type RemoveShardsArgs struct {
	ConfigNum int
	ShardIds  []int // 需要删除的分片号
}

type RemoveShardsReply struct {
	Err Err
}

type RestoreShardsArgs struct {
	ConfigNum int
	ShardIds  []int // 需要删除的分片号
}
