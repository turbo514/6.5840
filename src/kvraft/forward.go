package kvraft

func (kv *KVServer) forwardMsg() {
	for {
		select {
		case <-kv.closeCh:
			return
		case msg := <-kv.applyCh: // FIXME: 改造成并发?不过要按照确定的顺序的话可能就不该搞并发?
			if msg.CommandValid { // 检查是否是客户端命令
				index := msg.CommandIndex

				op := msg.Command.(Op)

				if msg.CommandIndex <= kv.lastApplied {
					continue
				}

				// 幂等检查
				// 对于GET请求,没有任何影响,现在的需要返回,未来的需要应用
				// 对于PUT请求,过去的和现在的不能应用,现在的需要返回,未来的需要应用
				// 对于APPEND请求,过去和现在的都不能应用,现在的需要返回,未来的需要应用
				// 对于不需要应用的请求,我选择不在该协程函数回应
				kv.mu.Lock()
				if op.MsgId <= kv.getSeq(op.ClientId) {
					//EPrintf("请求未通过幂等检查,op=%+v", op)
					kv.mu.Unlock()
					continue
				}
				kv.mu.Unlock()

				// 请求应用
				switch op.OpType {
				case GET:
					op.Value = kv.data[op.Key]
					kv.mu.Lock()
					kv.history[op.ClientId] = op.Value
					kv.mu.Unlock()
				case PUT:
					kv.data[op.Key] = op.Value
				case APPEND:
					kv.data[op.Key] += op.Value
				}

				kv.mu.Lock()
				notifyCh := kv.notifyChMap[index]
				kv.mu.Unlock()

				kv.mu.Lock()
				kv.seqMap[op.ClientId] = op.MsgId
				kv.mu.Unlock()

				// 只有与Clerk通信的leader节点需要回应
				if term, isLeader := kv.rf.GetState(); isLeader && term == msg.CommandTerm {
					EPrintf("[%d] leader应用了[%d]请求,op=%+v", kv.me, index, op)
					notifyCh <- op
				}

				if kv.needSnapshot() {
					kv.mu.Lock()
					kv.makeSnapshot(msg.CommandIndex)
					kv.mu.Unlock()
				}

			} else if msg.SnapshotValid { // 检查是否是安装快照命令
				kv.mu.Lock()
				kv.readPersist(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}
