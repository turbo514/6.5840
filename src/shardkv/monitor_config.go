package shardkv

import (
	"sync"
	"time"
)

const addConfigMaxWait = 500 * time.Millisecond
const addShardMaxWait = 500 * time.Millisecond
const deleteShardMaxWait = 500 * time.Millisecond
const restoreShardMaxWait = 500 * time.Millisecond

// Leader 节点会定期向 ShardCtrler 询问最新配置信息并更新自身分片情况
func (kv *ShardKV) monitorUpdateConfigFunc() {
	ticker := time.NewTicker(time.Millisecond * 100) // FIXME: 随机化会比较好?可以减轻同一时刻的大量请求打在shardctrler上?
	defer ticker.Stop()

	for {
		select {
		case <-kv.closeCh:
			return
		case <-ticker.C:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}
			kv.mu.Lock()
			// 检查当前是否没有分片信息正在更改
			isProcessShardCommand := false
			for _, shard := range kv.shards {
				if shard.State != SERVING {
					isProcessShardCommand = true
					break
				}
			}
			// 若当前正在更改自身配置则放弃本次询问
			if isProcessShardCommand {
				kv.mu.Unlock()
				continue
			}
			currentConfigNum := kv.currentConfig.Num
			DPrintf("[%d:%d] 开始检查有无新配置,当前版本号是%d", kv.me, kv.gid, currentConfigNum)
			kv.mu.Unlock()

			newConfig := kv.clerk.Query(currentConfigNum + 1)
			if newConfig.Num == currentConfigNum+1 { // 有可能当前就是最新配置
				DPrintf("[%d:%d] 找到版本号为%d的新配置,准备添加", kv.me, kv.gid, newConfig.Num)
				cmd := CommandArgs{
					CommandType: ADDCONFIG,
					Data:        newConfig,
				}
				var result CommandResult
				kv.StartCommand(cmd, &result, addConfigMaxWait)
			}
		}
	}
}

// 定期检查是否需要向其它副本组拉取分片
func (kv *ShardKV) monitorPullFunc() {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for {
		select {
		case <-kv.closeCh:
			return
		case <-ticker.C:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}

			kv.mu.Lock()
			gidToShards := kv.getGidToShards(PULLING)
			if len(gidToShards) == 0 {
				kv.mu.Unlock()
				continue
			}

			num := kv.currentConfig.Num
			DPrintf("[%d:%d] 开始拉取分片,num=%d,shards:%+v", kv.me, kv.gid, num, gidToShards)
			wg := &sync.WaitGroup{}
			wg.Add(len(gidToShards))
			for gid, shardIds := range gidToShards {
				servers := kv.lastConfig.Groups[gid]
				go func(gid int, shardIds []int) {
					defer wg.Done()
					kv.PullShards(num, gid, shardIds, servers)
				}(gid, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
	}
}

// 通知某个组可以删除分片了
func (kv *ShardKV) monitorGCFunc() {
	ticker := time.NewTicker(100 * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-kv.closeCh:
			return
		case <-ticker.C:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}

			kv.mu.Lock()
			gidToShards := kv.getGidToShards(SERVINGWITHGC)
			if len(gidToShards) == 0 {
				kv.mu.Unlock()
				continue
			}

			num := kv.currentConfig.Num
			DPrintf("[%d:%d] 开始通知删除分片,num=%d,gidToShards:%+v", kv.me, kv.gid, num, gidToShards)
			wg := &sync.WaitGroup{}
			wg.Add(len(gidToShards))
			for gid, shardIds := range gidToShards {
				servers := kv.lastConfig.Groups[gid]
				go func(gid int, shardIds []int) {
					defer wg.Done()
					kv.GCShards(num, gid, shardIds, servers)
				}(gid, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
	}
}

// 从某个组拉取分片
func (kv *ShardKV) PullShards(num int, gid int, shardIds []int, servers []string) {
	args := GetShardsArgs{
		ConfigNum: num,
		Gid:       gid,
		ShardIds:  shardIds,
	}

	for _, server := range servers {
		//DPrintf("[%d:%d] 向[%d:%d]拉取分片,shardIds:%+v", kv.me, kv.gid, i, gid, shardIds)
		reply := GetShardsReply{}
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.GetShards", &args, &reply)
		if ok && reply.Err == OK {
			reply.ConfigNum = num // CONFUSED: ??
			command := CommandArgs{
				CommandType: ADDSHARD,
				Data:        reply,
			}
			kv.StartCommand(command, &CommandResult{}, addShardMaxWait)
			return
		}
	}

	//kv.signPull()
}

func (kv *ShardKV) GCShards(num int, gid int, shardIds []int, servers []string) {
	args := RemoveShardsArgs{
		ConfigNum: num,
		ShardIds:  shardIds,
	}

	for _, server := range servers {
		//DPrintf("[%d:%d] 向[%d:%d]发送删除分片请求,shardIds:%+v", kv.me, kv.gid, i, gid, shardIds)
		reply := RemoveShardsReply{}
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.RemoveShards", &args, &reply)
		if ok && reply.Err == OK {
			cmd := CommandArgs{
				CommandType: RESTORESHARD,
				Data: RestoreShardsArgs{
					ConfigNum: args.ConfigNum,
					ShardIds:  args.ShardIds,
				},
			}
			kv.StartCommand(cmd, &CommandResult{}, restoreShardMaxWait)
			break
		}
	}
}

// 获取状态为state的分片,按组分类
func (kv *ShardKV) getGidToShards(state ShardState) map[int][]int {
	gidToShards := make(map[int][]int)
	for shardId, shard := range kv.shards {
		if shard.State == state {
			gid := kv.lastConfig.Shards[shardId]
			gidToShards[gid] = append(gidToShards[gid], shardId)
		}
	}
	return gidToShards
}

// func (kv *ShardKV) signPull() {
// 	select {
// 	case kv.pullNotifier <- struct{}{}:
// 	default:
// 	}
// }

// func (kv *ShardKV) signGC() {
// 	select {
// 	case kv.gcNotifier <- struct{}{}:
// 	default:
// 	}
// }
