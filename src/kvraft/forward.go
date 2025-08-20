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

				// 幂等检查
				// 对于GET请求,没有任何影响,现在的需要返回,未来的需要应用
				// 对于PUT请求,过去的和现在的不能应用,现在的需要返回,未来的需要应用
				// 对于APPEND请求,过去和现在的都不能应用,现在的需要返回,未来的需要应用
				// 对于不需要应用的请求,我选择不在该协程函数回应
				kv.seqLock.RLock()
				if op.MsgId <= kv.getSeq(op.ClientId) {
					//EPrintf("请求未通过幂等检查,op=%+v", op)
					kv.seqLock.RUnlock()
					continue
				}
				kv.seqLock.RUnlock()

				// 请求应用
				switch op.OpType {
				case GET:
					op.Value = kv.data[op.Key]
					kv.historyLock.Lock()
					kv.history[op.ClientId] = op.Value
					kv.historyLock.Unlock()
				case PUT:
					kv.data[op.Key] = op.Value
				case APPEND:
					kv.data[op.Key] += op.Value
				}

				kv.notifyLock.RLock()
				notifyCh := kv.notifyChMap[index]
				kv.notifyLock.RUnlock()

				kv.seqLock.Lock()
				kv.seqMap[op.ClientId] = op.MsgId
				kv.seqLock.Unlock()

				// 只有与Clerk通信的leader节点需要
				if term, isLeader := kv.rf.GetState(); isLeader && term == msg.CommandTerm {
					EPrintf("[%d] leader应用了[%d]请求,op=%+v", kv.me, index, op)
					notifyCh <- op
				}
			} else if msg.SnapshotValid { // 检查是否是安装快照命令
				// ...
			}
		}
	}
}
