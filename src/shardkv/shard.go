package shardkv

type ShardState int32

const (
	SERVING       ShardState = 1 // 当前分片正常服务中
	PULLING       ShardState = 2 // 当前分片正在从其它复制组中拉取信息
	BEPULLING     ShardState = 3 // 当前分片正在复制给其它复制组
	SERVINGWITHGC ShardState = 4 // 当前分片正在等待清除（监视器检测到后需要从拥有这个分片的复制组中删除分片）
	REMOVED       ShardState = 5
)

type Shard struct {
	ShardKVDB map[string]string
	State     ShardState
}

func (s *Shard) get(key string) string {
	return s.ShardKVDB[key]
}

func (s *Shard) put(key string, value string) {
	s.ShardKVDB[key] = value
}

func (s *Shard) append(key string, value string, logString string) {
	s.ShardKVDB[key] += value
	//fmt.Println(logString)
}

func (s *Shard) state() ShardState {
	return s.State
}

func (s *Shard) clone() Shard {
	shardClone := make(map[string]string, len(s.ShardKVDB))
	for k, v := range s.ShardKVDB {
		shardClone[k] = v
	}
	return Shard{
		ShardKVDB: shardClone,
		State:     SERVINGWITHGC,
	}
}

func makeShard(state ShardState) *Shard {
	return &Shard{
		ShardKVDB: make(map[string]string),
		State:     state,
	}
}
