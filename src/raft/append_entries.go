package raft

import "time"

type AppendEntriesArgs struct {
	Term     int // leader的任期
	LeaderId int // leader的id
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号，以便于leader去更新自己的任期号
	Success bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
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
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	DPrintf("[%d] 收到选期更大的AppendEntries,转变为follower", rf.me)
	rf.setRole(FOLLOWER)
	rf.lastHeartbeat = time.Now()
}
