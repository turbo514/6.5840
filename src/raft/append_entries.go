package raft

import (
	"sort"
	"time"
)

type AppendEntriesArgs struct {
	Term     int // leader的任期
	LeaderId int // leader的id

	PrevLogIndex int     // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int     // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []Entry // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int     // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号，以便于leader去更新自己的任期号
	Success bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true

	// 用于快速恢复
	Xterm  int // 冲突条目的任期号 (如果存在)
	Xindex int // 冲突任期的第一个条目索引
	Xlen   int // Follower 的日志长度
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// 如果领导人的任期小于接收者的当前任期,返回false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		// 如果领导人的任期大于接收者(无论接收者是谁)的当前任期
		// 变为Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	//	DPrintf("[%d] 收到选期更大的AppendEntries,转变为follower", rf.me)
	rf.setRole(FOLLOWER)
	rf.lastHeartbeat = time.Now()

	reply.Term = rf.currentTerm

	// 检查日志
	if args.PrevLogIndex < rf.log.zeroLogIndex {
		EPrintf("[%d] 日志索引[%d]过小,zeroLogIndex=%d", rf.me, args.PrevLogIndex, rf.log.zeroLogIndex)
		return
	} else if args.PrevLogIndex > rf.getLastLogIndex() {
		EPrintf("[%d] 日志索引[%d]过大,lastLogIndex=%d", rf.me, args.PrevLogIndex, rf.getLastLogIndex())
		reply.Xterm = -1
		reply.Xlen = rf.getLastLogIndex()
		return
	}

	// 索引相同的日志,term也应当相同
	// 否则选择覆盖
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		EPrintf("[%d] 日志不合规,索引[%d]term不同,应当是%d,实际为%d)",
			rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.getLogTerm(args.PrevLogIndex))
		reply.Xterm = rf.getLogTerm(args.PrevLogIndex)
		reply.Xindex = rf.getLogIndex(sort.Search(len(rf.log.entries), func(i int) bool {
			return rf.log.entries[i].Term >= reply.Xterm
		}))
		return
	}

	// 日志匹配成功
	// 添加(覆盖)日志
	for i := 0; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i // 获取日志索引
		if index > rf.getLastLogIndex() {
			// 日志缺失,直接追加
			rf.log.entries = append(rf.log.entries, args.Entries[i:]...)
			break
		}
		if rf.getLogTerm(index) == args.Entries[i].Term {
			// 存在且一致,不做处理
			continue
		} else {
			// 存在但不一致,覆盖追加
			rf.log.entries = append(rf.log.entries[:rf.getIndex(index)], args.Entries[i:]...)
			break
		}
	}
	//EPrintf("[%d] 覆盖后entreis长度为%d", rf.me, len(rf.log.entries))

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = getMin(args.LeaderCommit, rf.getLastLogIndex())
	}

	reply.Success = true
}

func getMin(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func getMax(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// 未使用
// func (rf *Raft) AppendEntriesToPeers() {
// 	for i := range rf.peers {
// 		if i == rf.me {
// 			continue
// 		}
// 		go func(i int) {
// 			var reply AppendEntriesReply
// 			if ok := rf.sendAppendEntries(i, &args, &reply); ok {
// 				// 会出现并发
// 				// 一把大锁保平安
// 				rf.mu.Lock()
// 				if rf.getRole() == LEADER && reply.Term > rf.currentTerm {
// 					rf.currentTerm = reply.Term
// 					rf.votedFor = -1
// 					DPrintf("[%d] leader发现选期更大的节点,转变为follower", rf.me)
// 					rf.setRole(FOLLOWER)
// 				}
// 				rf.mu.Unlock()
// 			}
// 		}(i)
// 	}
// }
