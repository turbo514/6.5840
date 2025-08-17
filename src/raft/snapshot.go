package raft

import "time"

type InstallSnapshotArgs struct {
	Term             int // leader的任期
	LeaderId         int // 领导人的 ID，以便于跟随者重定向请求
	LastIncludeIndex int // 快照中包含的最后日志条目的索引值
	LastIncludeTerm  int // 快照中包含的最后日志条目的任期号

	//Offset int    // 分块在快照中的字节偏移量
	Data []byte // 从偏移量开始的快照分块的原始字节
	//Done   bool   // 如果这是最后一个分块则为 true
}

type InstallSnapshotReply struct {
	Term int // 当前任期号，便于领导人更新自己
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查是否是当前leader发送的
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term

	if args.Term > rf.currentTerm {
		// 转换为follower
		rf.setRole(FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.lastHeartbeat = time.Now()

	// 如果发送的快照所包含的数据并不新于自己的快照,舍弃
	if args.LastIncludeIndex <= rf.getLastIncludeIndex() {
		return
	}

	// 检查发送的快照的进度是否小于自己的进度
	if args.LastIncludeIndex < rf.getLastLogIndex() {
		index := rf.getIndex(args.LastIncludeIndex + 1)
		rf.log = newLog(args.LastIncludeIndex, args.LastIncludeTerm, rf.log.Entries[index:])
	} else {
		rf.log = newLog(args.LastIncludeIndex, args.LastIncludeTerm, nil)
	}

	// 刷新lastApplied和commitIndex
	if rf.lastApplied < args.LastIncludeIndex {
		rf.lastApplied = args.LastIncludeIndex
	}
	if rf.commitIndex < args.LastIncludeIndex {
		rf.commitIndex = args.LastIncludeIndex
	}

	// 持久化
	rf.persister.Save(rf.encodeState(), args.Data)

	// 使用快照重置状态机
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
}

func (rf *Raft) installSnapshotOnPeer(server int, arg *InstallSnapshotArgs) {
	snapshot := arg.Data
	if len(snapshot) == 0 {
		// debug用不过我认为这不会发生
		panic("snap未准备好,放弃发送")
	}

	var reply InstallSnapshotReply
	if ok := rf.sendInstallSnapshot(server, arg, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.getRole() == LEADER && rf.currentTerm == arg.Term {
			if reply.Term > rf.currentTerm {
				DPrintf("[%d] leader发现选期更大的节点,转变为follower", rf.me)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.setRole(FOLLOWER)
				rf.persist()
			} else {
				rf.matchIndex[server] = arg.LastIncludeIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
		}
	}
}
