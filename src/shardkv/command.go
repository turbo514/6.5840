package shardkv

import "time"

type CommandArgs struct {
	CommandType int32
	Data        interface{}
}

type CommandResult struct {
	Err   Err
	Value interface{}
}

func (kv *ShardKV) StartCommand(cmd CommandArgs, result *CommandResult, maxWait time.Duration) {
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		result.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := make(chan CommandResult, 1)
	kv.notifyChMap[index] = notifyCh
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChMap, index)
		kv.mu.Unlock()
	}()

	timeout := time.After(maxWait)

	select {
	case <-kv.closeCh:
		result.Err = ErrClosed
	case res := <-notifyCh:
		result.Value, result.Err = res.Value, res.Err
	case <-timeout:
		result.Err = ErrTimeout
	}
}

// 检查当前分片是否仍由自己负责并且当前分片的状态正常
func (kv *ShardKV) checkShardAndState(shardId int) bool {
	shard, ok := kv.shards[shardId]
	if ok && kv.currentConfig.Shards[shardId] == kv.gid &&
		(shard.State == SERVING || shard.State == SERVINGWITHGC) {
		return true
	}
	return false
}

func (kv *ShardKV) isInvalidRequest(clientId int64, requestId int64) bool {
	return requestId <= kv.seqMap[clientId]
}
