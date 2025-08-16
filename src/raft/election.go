package raft

import "time"

type RequestVoteArgs struct {
	Term        int // 候选人的任期
	CandidateID int // 候选人的编号

	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 为真时候选人赢得了此张选票
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()         // 考虑这么一种情况,在处理RequestVote请求时,刚好开启了新一轮选举,此时会更新currentTerm(follower还会更新角色和投票给谁)
	defer rf.mu.Unlock() // 就有可能导致误投票,造成投了两票的错误

	if args.Term < rf.currentTerm { // 检查选举是否已过时
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm { // 如果遇到更大任期的选票,需要变为follower并重置
		DPrintf("[%d] RequestVote收到选期更大的选票[%d],转变为follower", rf.me, args.Term)
		rf.setRole(FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist() // FIXME: 有概率进行了两次持久化,这应该是一个需要优化的点..但是为了代码整洁性..
	} else {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateID { // 检查本轮是否还能投票
			reply.Term = rf.currentTerm
			return
		}
	}

	// 检查日志情况
	// 投票人会拒绝掉那些日志没有自己新的投票请求
	// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新
	// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新
	if args.LastLogTerm < rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()) {
		EPrintf("[%d] 接收到来自[%d]的选票,但日志情况不通过.LastLogTerm=%d,rf.getLastLogTerm=%d,LastLogIndex=%d,rf.getLastLogIndex=%d",
			rf.me, args.CandidateID, args.LastLogTerm, rf.getLastLogTerm(), args.LastLogIndex, rf.getLastLogIndex())
		return
	}

	// 投票,并重置follower超时器
	DPrintf("[%d] 投票给 [%d]", rf.me, args.CandidateID)
	EPrintf("[%d] 投票给 [%d]", rf.me, args.CandidateID)
	rf.votedFor = args.CandidateID
	rf.lastHeartbeat = time.Now()
	rf.persist() // FIXME: 第二次持久化

	reply.Term = args.Term
	reply.VoteGranted = true
}

func (rf *Raft) startElection(term, lastLogIndex, lastLogTerm int) <-chan *RequestVoteReply {
	ch := make(chan *RequestVoteReply, len(rf.peers))

	DPrintf("[%d] 开始了[%d]的选期", rf.me, term)

	args := RequestVoteArgs{
		Term:        term,
		CandidateID: rf.me,

		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			var reply RequestVoteReply
			if ok := rf.sendRequestVote(i, &args, &reply); !ok {
				reply.Term = term
				reply.VoteGranted = false
			}
			ch <- &reply
		}(i)
	}

	return ch
}
