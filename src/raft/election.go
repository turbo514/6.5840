package raft

import "time"

type RequestVoteArgs struct {
	Term        int // 候选人的任期
	CandidateID int // 候选人的编号
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
		rf.votedFor = -1
		// rf.lastHeartbeat = time.Now()
	} else {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateID { // 检查本轮是否还能投票
			return
		}
	}

	// 投票,并重置follower超时器
	DPrintf("[%d] 投票给 [%d]", rf.me, args.CandidateID)
	rf.votedFor = args.CandidateID
	rf.currentTerm = args.Term
	rf.lastHeartbeat = time.Now()

	reply.Term = args.Term
	reply.VoteGranted = true
}

func (rf *Raft) startElection(term int) <-chan *RequestVoteReply {
	ch := make(chan *RequestVoteReply, len(rf.peers))

	DPrintf("[%d] 开始了[%d]的选期", rf.me, term)

	args := RequestVoteArgs{
		Term:        term,
		CandidateID: rf.me,
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
