package shardctrler

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // 配置版本号
	Shards [NShards]int     // shard -> gid 分片分配给了哪个副本组
	Groups map[int][]string // gid -> servers[] 副本组gid映射到服务器地址
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrClosed      = "ErrClosed"
)

const (
	JOIN  int32 = 1
	MOVE  int32 = 2
	LEAVE int32 = 3
	QUERY int32 = 4
)

type Err string

type DeduplicateArgs struct {
	ClientId  int64
	RequestId int64
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	DeduplicateArgs
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	DeduplicateArgs
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	DeduplicateArgs
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	DeduplicateArgs
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
