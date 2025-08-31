package shardkv

import (
	"fmt"

	"6.5840/shardctrler"
)

func (kv *ShardKV) updateSeq(clientId, requestId int64) {
	kv.seqMap[clientId] = requestId
}

func (kv *ShardKV) processGet(args *GetArgs, reply *CommandResult) {
	// 检查该分片是否由自己负责
	shardId := key2shard(args.Key)
	if !kv.checkShardAndState(shardId) {
		// DPrintf([%d:%d]不负责分片[%s]", kv.me, kv.gid, args.Key)
		reply.Err = ErrWrongGroup
		return
	}

	reply.Err = OK
	if kv.isInvalidRequest(args.ClientId, args.RequestId) {
		//reply.Value = kv.history[args.ClientId]
		reply.Value = kv.shards[shardId].get(args.Key)
	} else {
		value := kv.shards[shardId].get(args.Key)
		//kv.history[args.ClientId] = value
		reply.Value = value
		kv.updateSeq(args.ClientId, args.RequestId)
	}
	//fmt.Printf("[%d:%d] Get到了新值,key=%s,value=%s\n", kv.me, kv.gid, args.Key, reply.Value.(string))
}

func (kv *ShardKV) processPutAppend(args *PutAppendArgs, reply *CommandResult) {
	shardId := key2shard(args.Key)
	// 检查该分片是否由自己负责
	if !kv.checkShardAndState(shardId) {
		// DPrintf([%d:%d]不负责分片[%s]", kv.me, kv.gid, args.Key)
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err = OK
	if !kv.isInvalidRequest(args.ClientId, args.RequestId) {
		if args.Op == PUT {
			kv.shards[shardId].put(args.Key, args.Value)
			//fmt.Printf("[%d:%d] put了新值,key=%s,value=%s\n", kv.me, kv.gid, args.Key, args.Value)
		} else {
			kv.shards[shardId].append(args.Key, args.Value,
				fmt.Sprintf("[%d:%d] append,key=%s,value=%s", kv.me, kv.gid, args.Key, args.Value))
			//fmt.Printf("[%d:%d] append了新值,key=%s,value=%s\n", kv.me, kv.gid, args.Key, args.Value)
		}
		kv.updateSeq(args.ClientId, args.RequestId)
	}
}

func (kv *ShardKV) processAddConfig(newConfig *shardctrler.Config, reply *CommandResult) {
	if newConfig.Num == kv.currentConfig.Num+1 {
		// 检查新旧配置的差异
		for i := 0; i < shardctrler.NShards; i++ {
			if newConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] != kv.gid {
				// 获得了新分片
				if kv.currentConfig.Shards[i] != 0 {
					kv.shards[i].State = PULLING // 从其他组拉取该切片的内容
				}
			}
			if newConfig.Shards[i] != kv.gid && kv.currentConfig.Shards[i] == kv.gid {
				// 分片被移除
				if newConfig.Shards[i] != 0 {
					kv.shards[i].State = BEPULLING // 等待其他组拉取该切片
				}
			}
		}

		kv.lastConfig, kv.currentConfig = kv.currentConfig, *newConfig
		DPrintf("[%d:%d] 已应用新配置[%d]", kv.me, kv.gid, newConfig.Num)

		reply.Err = OK

		//kv.signPull()
	} else {
		//reply.Err = ErrDuplicate ??
		reply.Err = OK // ??
	}
}

func (kv *ShardKV) processAddShards(args *GetShardsReply, reply *CommandResult) {
	//DPrintf("[%d:%d] 准备添加新分片,num=%d", kv.me, kv.gid, args.ConfigNum)
	if args.ConfigNum == kv.currentConfig.Num {
		// 添加分片
		shards := args.Shards
		for shardId := range shards {
			newShard := shards[shardId]
			oldShard := kv.shards[shardId]
			if oldShard.State == PULLING { // 以此实现幂等(避免不正确地覆盖)
				for key, value := range newShard.ShardKVDB {
					oldShard.ShardKVDB[key] = value
				}
				oldShard.State = SERVINGWITHGC
			}
		}

		// 添加相应的Seq
		for clientId, requestId := range args.SeqMap {
			if seq := kv.seqMap[clientId]; requestId >= seq {
				kv.seqMap[clientId] = requestId
				//kv.history[clientId] = args.History[clientId]
			}
		}

		reply.Err = OK
		DPrintf("[%d:%d] 添加了新分片,num=%d,shard=%+v", kv.me, kv.gid, args.ConfigNum, args.Shards)

		//kv.signGC()
	} else {
		reply.Err = OK // ??
	}
	//DPrintf("[%d:%d] 添加新分片完成,num=%d")
}

func (kv *ShardKV) processRemoveShards(args *RemoveShardsArgs, reply *CommandResult) {
	if args.ConfigNum == kv.currentConfig.Num {
		// 从被拉取状态恢复
		for _, shardId := range args.ShardIds {
			if shard, ok := kv.shards[shardId]; ok && shard.State == BEPULLING {
				kv.shards[shardId] = makeShard(SERVING)
			}
		}
		DPrintf("[%d:%d] 移除了分片,num=%d,shardIds=%+v", kv.me, kv.gid, args.ConfigNum, args.ShardIds)
		reply.Err = OK
	} else if args.ConfigNum < kv.currentConfig.Num {
		reply.Err = OK
	} else {
		reply.Err = ErrOther
	}
}

// 在通知其他组移除不属于它们的分片后,消除标记
func (kv *ShardKV) processRestoreShards(args *RestoreShardsArgs, reply *CommandResult) {
	if args.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range args.ShardIds {
			if kv.shards[shardId].State == SERVINGWITHGC { // ?
				kv.shards[shardId].State = SERVING
			}
		}
		DPrintf("[%d:%d] 消除了分片标记,num=%d,shardIds=%+v", kv.me, kv.gid, args.ConfigNum, args.ShardIds)
	}
}

func (kv *ShardKV) processGetShards(args *GetShardsArgs, reply *CommandResult) {
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	if args.ConfigNum == kv.currentConfig.Num {
		reply.Err = OK

		res := GetShardsReply{}

		res.Shards = make(map[int]Shard, len(args.ShardIds))
		for _, shardId := range args.ShardIds {
			res.Shards[shardId] = kv.shards[shardId].clone()
		}

		res.SeqMap = make(map[int64]int64, len(kv.seqMap))
		for key, value := range kv.seqMap {
			res.SeqMap[key] = value
		}

		reply.Value = res

	} else {
		reply.Err = ErrWrongGroup
	}
}
