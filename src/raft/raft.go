package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	closeCh chan struct{}

	applyCh       chan ApplyMsg // Raft通过该通道发送ApplyMsg消息
	applyNotifier sync.Cond

	lastHeartbeat time.Time // 超时标志,若超时则变为candidate并开始选举
	currentTerm   int       // 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor      int       // 当前任期内投票给的候选人的Id，如果没有投给任何候选人则为-1
	role          int32     // 当前身份,原子变量防止并发问题

	log         Log // 日志条目, 每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
	commitIndex int // 已知已提交（到日志中）的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	// 选举后重新初始化
	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
}

// 返回当前节点的任期,以及它是否认为自己是leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.getRole() == LEADER

	return term, isleader
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// 第一个返回值是该命令如果最终被提交时将出现的索引
// 第二个返回值是当前任期
// 第三个返回值是当且节点是否为Leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm      // 当前任期
	index := -1                 // 日志索引
	if rf.getRole() != LEADER { // 如果当前节点不是leader
		return term, index, false
	}

	// 若是Leader,则添加日志条目
	rf.appendEntries(term, command)

	index = rf.getLastLogIndex()
	rf.matchIndex[rf.me]++
	EPrintf("[%d] 客户端提交了日志给leader,index=%d,term=%d", rf.me, index, term)
	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.closeCh)
}

func (rf *Raft) run() {
	rf.mu.Lock() // 默认阻塞进入该函数
	// 什么身份就该做什么身份的事,防止并发危机
	defer rf.mu.Unlock()

	for {
		select {
		case <-rf.closeCh:
			return
		default:
			switch rf.getRole() {
			case FOLLOWER:
				DPrintf("[%d] 进入runFollower", rf.me)
				EPrintf("[%d] 进入runFollower", rf.me)
				rf.runFollower()
			case CANDIDATE:
				DPrintf("[%d] 进入runCandidate", rf.me)
				EPrintf("[%d] 进入runCandidate", rf.me)
				rf.runCandidate()
			case LEADER:
				DPrintf("[%d] 进入runLeader", rf.me)
				rf.runLeader()
			}
		}
	}
}

func (rf *Raft) runFollower() {
	for {
		// 重置计时器
		maxTimeout := time.Millisecond * time.Duration((150 + rand.Int63n(200)))
		notify := time.After(maxTimeout)
		rf.mu.Unlock()

		select {
		case <-rf.closeCh:
			rf.mu.Lock()
			return
		case <-notify: //
			rf.mu.Lock()

			// 检查是否超时
			if time.Now().Before(rf.lastHeartbeat.Add(maxTimeout)) {
				// 未超时
				continue
			}

			// 超时,开始新一轮选举
			rf.setRole(CANDIDATE)
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	rf.mu.Unlock()

	// 有可能在这期间发生了身份的变化
	// 若发生了变化,则要消除不属于现在身份的所有行为的影响

	votech, count := rf.startElection(currentTerm, lastLogIndex, lastLogTerm), 1

	timeout := time.After(time.Millisecond * time.Duration((150 + rand.Int63n(200))))
	for {
		select {
		case <-rf.closeCh:
			rf.mu.Lock()
			return
		case <-timeout:
			// 选举超时,此时的身份不清楚,而且选择不在这里判断(至少不会是Leader)
			rf.mu.Lock()
			return
		case reply := <-votech:
			rf.mu.Lock()

			// 有可能现在已经不是Candidate了(至少应该不是leader)
			if rf.getRole() == FOLLOWER {
				return
			}

			if reply.Term > rf.currentTerm {
				// 当前选举已过时
				// 切换为Follower并重置
				DPrintf("[%d] 收到选期更大的选票: %d", rf.me, reply.Term)
				rf.setRole(FOLLOWER)
				rf.currentTerm = reply.Term
				rf.votedFor = -1

				return
			}

			if reply.VoteGranted { // 检查选票情况
				count++
				if count > len(rf.peers)/2 {
					// 获得大多数选票,成为leader
					rf.setRole(LEADER)
					return
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) runLeader() {
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}

	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	rf.mu.Unlock()

	for {
		// 检查自己是否还是leader
		if rf.getRole() != LEADER {
			rf.mu.Lock()
			DPrintf("[%d] 从leader退位,当前选期[%d]", rf.me, rf.currentTerm)
			return
		}

		rf.mu.Lock()
		// 更新leader的CommitIndex
		rf.updateCommitIndex()
		rf.mu.Unlock()

		// 开始AppendEntries
		select {
		case <-rf.closeCh:
			rf.mu.Lock()
			return
		default:
			rf.mu.Lock() // 或许可以去掉锁?然后改成协程发送一轮?
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,

					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.getLogTerm(rf.nextIndex[i] - 1),
					Entries:      rf.getAppendEntries(rf.nextIndex[i]),
					LeaderCommit: rf.commitIndex,
				}

				go func(i int) {
					var reply AppendEntriesReply
					if ok := rf.sendAppendEntries(i, &args, &reply); ok {
						// 会出现并发
						// 一把大锁保平安
						rf.mu.Lock()
						if rf.getRole() == LEADER {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								DPrintf("[%d] leader发现选期更大的节点,转变为follower", rf.me)
								rf.setRole(FOLLOWER)
							} else {
								if !reply.Success {
									rf.nextIndex[i]--
								} else {
									rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
									rf.nextIndex[i] = rf.matchIndex[i] + 1
									//EPrintf("[%d] 发送日志给follower[%d]成功,matchIndex=%d,nextIndex=%d",
									//	rf.me, i, rf.matchIndex[i], rf.nextIndex[i])
								}
							}
						}
						rf.mu.Unlock()
					}
				}(i)
			}
			rf.mu.Unlock()
		}

		// 有可能在此期间收到更加新的选期的投票请求或者心跳
		time.Sleep(100 * time.Millisecond)
	}

}

// 创建一个Raft节点
// applyCh是测试者或服务预期Raft通过该通道发送ApplyMsg消息的通道
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		closeCh:   make(chan struct{}),
		applyCh:   applyCh,

		lastHeartbeat: time.Now(),

		currentTerm: 0,
		votedFor:    -1,
		role:        FOLLOWER,

		commitIndex: 0,
		lastApplied: 0,
		log:         newLog(),
	}

	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	go rf.applyMsgFunc()

	return rf
}
