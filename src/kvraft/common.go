package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type PutAppendArgs struct {
	OpType   int32
	Key      string
	Value    string
	ClientId int64
	MsgId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	MsgId    int64
}

type GetReply struct {
	Err   Err
	Value string
}

const (
	GET    int32 = 1
	PUT    int32 = 2
	APPEND int32 = 3
)
