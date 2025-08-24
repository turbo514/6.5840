package raft

import (
	"time"
)

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
	//log.Printf("[%d] 接收到了来自[%d]的安装快照请求,snapshotIndex=%d,lastIncludeIndex=%d\n",
	//	rf.me, args.LeaderId, args.LastIncludeIndex, rf.getLastIncludeIndex())
	// 检查是否是当前leader发送的
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
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

	// 如果发送的快照所包含的数据并不新于自己的快照或并不新于自己的状态机,舍弃
	if args.LastIncludeIndex <= rf.getLastIncludeIndex() || args.LastIncludeIndex <= rf.lastApplied {
		rf.mu.Unlock()
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

	//log.Printf("[%d] 已更新,lastApplied=%d,commitIndex=%d,LastIncludeIndex=%d\n", rf.me, rf.lastApplied, rf.commitIndex, rf.getLastIncludeIndex())

	// 持久化
	rf.persister.Save(rf.encodeState(), args.Data)

	// 使用快照重置状态机
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}

	//log.Printf("[%d] 日志安装成功,leaderid=%d,snapshotindex=%d\n",
	//	rf.me, args.LeaderId, args.LastIncludeIndex)
}

func (rf *Raft) installSnapshotOnPeer(server int, arg *InstallSnapshotArgs) {
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
