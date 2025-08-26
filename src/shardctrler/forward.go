package shardctrler

import (
	"sort"
)

func (sc *ShardCtrler) forwardMsg() {
	for {
		select {
		case <-sc.closeCh:
			return
		case msg := <-sc.applyCh:
			index := msg.CommandIndex
			op := msg.Command.(Op)

			// 幂等校验
			sc.mu.Lock()
			if sc.isInvalidRequest(op.ClientId, op.RequestId) {
				sc.mu.Unlock()
				continue
			}

			// 应用命令
			switch op.OpType {
			case JOIN:
				sc.processJoin(&op)
				DPrintf("[%d] JOIN,requestId=%d,servers=%+v,config=%+v", sc.me, op.RequestId, op.Servers, *sc.getLastConfig())
			case LEAVE:
				sc.processLeave(&op)
				DPrintf("[%d] LEAVE,requestId=%d,GIDs=%+v,config=%+v", sc.me, op.RequestId, op.GIDs, *sc.getLastConfig())
			case MOVE:
				sc.processMove(&op)
				DPrintf("[%d] MOVE,requestId=%d,Shard=%d,gid=%d,config=%+v", sc.me, op.RequestId, op.Shard, op.GID, *sc.getLastConfig())
			case QUERY:
				sc.processQuery(&op)
				DPrintf("[%d] QUERY,requestId=%d,config=%+v", sc.me, op.RequestId, *sc.getLastConfig())
				sc.history[op.ClientId] = &op.Configure
			}

			sc.seqMap[op.ClientId] = op.RequestId
			notifyCh, ok := sc.notifyChMap[index]
			sc.mu.Unlock()

			if term, isLeader := sc.rf.GetState(); ok && isLeader && term == msg.CommandTerm {
				notifyCh <- op
			}
		}
	}
}

func (sc *ShardCtrler) processJoin(op *Op) {
	lastConfig := sc.getLastConfig()

	//newShards := [NShards]int{} //切片数量可能会很高
	newGroups := cloneGroups(lastConfig.Groups)
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: newGroups,
		//Shards: ,
	}

	// 添加新servers
	for gid, server := range op.Servers {
		newGroups[gid] = server
	}

	// 调整切片分配
	migrateShards(&(newConfig.Shards), &(lastConfig.Shards), newGroups)

	// 添加新Config
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) processLeave(op *Op) {
	lastConfig := sc.getLastConfig()
	newGroups := cloneGroups(lastConfig.Groups)
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: newGroups,
	}

	// 移除副本组
	for _, gid := range op.GIDs {
		delete(newGroups, gid)
	}

	// 调整切片分配
	migrateShards(&(newConfig.Shards), &(lastConfig.Shards), newGroups)

	// 添加新Config
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) processMove(op *Op) {
	lastConfig := sc.getLastConfig()
	newGroups := cloneGroups(lastConfig.Groups) // CONFUSED: 是否可以复用而不是拷贝
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: newGroups,
	}

	copyShards(&(newConfig.Shards), &(lastConfig.Shards))

	newConfig.Shards[op.Shard] = op.GID

	// 添加新Config
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) processQuery(op *Op) {
	num := op.Num
	lastNum := sc.getLastConfig().Num
	if num >= lastNum || num == -1 {
		op.Configure = *sc.getLastConfig()
	} else {
		op.Configure = *sc.getConfig(num)
	}
}

func migrateShards(newShards, oldShards *[NShards]int, newGroups map[int][]string) {
	// 0.如果没有新的副本组,所有分片都分配给gid 0
	if len(newGroups) == 0 {
		return
	}

	// 1.统计gid -> shards的映射
	gidToShards := map[int][]int{}
	gids := make([]int, 0, len(newGroups))
	for gid := range newGroups {
		gidToShards[gid] = []int{}
		gids = append(gids, gid)
	}

	// 若切片所属的gid不存在于新配置中(newGroup),分配给gid 0
	for shard, gid := range oldShards {
		if _, exist := gidToShards[gid]; !exist {
			gidToShards[0] = append(gidToShards[0], shard)
		} else {
			gidToShards[gid] = append(gidToShards[gid], shard)
		}
	}

	// 2.为了确定性,对gid进行排序
	sort.Ints(gids)

	// 3.将所有待分配的分片暂时分配给最小的gid组
	min := gids[0]
	gidToShards[min] = append(gidToShards[min], gidToShards[0]...)
	gidToShards[0] = nil

	// 4.找到有哪些是过载的组,哪些是负载不足的组(升序排序)
	// 过载: 分片数量大于limit
	// 负载不足: 分片数量小于limit
	overload, lowload := []int{}, []int{}
	limit := NShards / len(newGroups)
	for _, gid := range gids {
		if len(gidToShards[gid]) > limit {
			overload = append(overload, gid)
		} else if len(gidToShards[gid]) < limit {
			lowload = append(lowload, gid)
		}
	}

	// 5.进行负载均衡
	// 若存在负载不足的组,则划分分片给它,让其拥有limit个分片
	for len(lowload) > 0 {
		lowgid := lowload[0]
		lowload = lowload[1:]
		lowgidGroup := gidToShards[lowgid]

		need := limit - len(lowgidGroup)

		for need > 0 { // 当负载均衡完成后,len(overload) 总是 >= 0
			overgid := overload[0]

			overgidGroup := gidToShards[overgid]
			n := len(overgidGroup)
			give := getMin(n-limit, need) // overgid组最多能给的分片数量
			need -= give

			lowgidGroup = append(lowgidGroup, overgidGroup[n-give:]...)

			// 从overgid组中移除被分配走的分片
			gidToShards[overgid] = overgidGroup[:n-give]
			if len(overgidGroup) == limit {
				overload = overload[1:]
			}
		}

		gidToShards[lowgid] = lowgidGroup
	}

	// 6. 根据更新后的gidToShards映射,生成 newShards数组
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
}

func getMin(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
