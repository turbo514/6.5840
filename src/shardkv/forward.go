package shardkv

import (
	"fmt"

	"6.5840/shardctrler"
)

func (kv *ShardKV) forawrdMsg() {
	for {
		select {
		case <-kv.closeCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				cmd := msg.Command.(CommandArgs)

				if msg.CommandIndex <= kv.snapshotIndex {
					fmt.Printf("[%d] msg.CommandIndex[%d]小于kv.LastApplied[%d]", kv.me, msg.CommandIndex, kv.snapshotIndex)
					continue
				}

				// 应用命令
				reply := CommandResult{}
				kv.mu.Lock()
				switch cmd.CommandType {
				case GET:
					args := cmd.Data.(GetArgs)
					kv.processGet(&args, &reply)
				case PUT:
					fallthrough
				case APPEND:
					args := cmd.Data.(PutAppendArgs)
					kv.processPutAppend(&args, &reply)
				case ADDCONFIG:
					config := cmd.Data.(shardctrler.Config)
					kv.processAddConfig(&config, &reply)
				case ADDSHARD:
					args := cmd.Data.(GetShardsReply)
					kv.processAddShards(&args, &reply)
				case REMOVESHARD:
					args := cmd.Data.(RemoveShardsArgs)
					kv.processRemoveShards(&args, &reply)
				case RESTORESHARD:
					args := cmd.Data.(RestoreShardsArgs)
					kv.processRestoreShards(&args, &reply)
				case GETSHARD:
					args := cmd.Data.(GetShardsArgs)
					kv.processGetShards(&args, &reply)
				}

				// 通知回复客户端
				notifyCh, ok := kv.notifyChMap[msg.CommandIndex]
				kv.mu.Unlock()
				if ok {
					if term, isLeader := kv.rf.GetState(); isLeader || msg.CommandTerm == term {
						notifyCh <- reply
					}
				}
				// if term, isLeader := kv.rf.GetState(); isLeader && msg.CommandTerm == term {
				// 	if cmd.CommandType == APPEND {
				// 		args := cmd.Data.(PutAppendArgs)
				// 		fmt.Printf("[:%d] append,key=%s,value=%s\n", kv.gid, args.Key, args.Value)
				// 	}
				// 	if ok {
				// 		notifyCh <- reply
				// 	}
				// }

				// 检查是否需要制作快照
				if kv.needSnapshot() {
					kv.mu.Lock()
					kv.makeSnapshot(msg.CommandIndex)
					kv.snapshotIndex = msg.CommandIndex
					kv.mu.Unlock()
				}

			} else if msg.SnapshotValid {
				kv.mu.Lock()
				DPrintf("[%d] 安装快照,snapshotIndex=%+d", kv.me, msg.SnapshotIndex)
				kv.readPersist(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}
