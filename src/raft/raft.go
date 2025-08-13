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

	lastHeartbeat time.Time // 超时标志,若超时则变为candidate并开始选举
	currentTerm   int       // 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor      int       // 当前任期内投票给的候选人的Id，如果没有投给任何候选人则为-1
	role          int32     // 当前身份,原子变量防止并发问题
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.getRole() == LEADER

	return term, isleader
}

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.closeCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
				rf.runFollower()
			case CANDIDATE:
				DPrintf("[%d] 进入runCandidate", rf.me)
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
	rf.mu.Unlock()

	// 有可能在这期间发生了身份的变化
	// 若发生了变化,则要消除不属于现在身份的所有行为的影响

	votech, count := rf.startElection(currentTerm), 1

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
	rf.mu.Unlock()

	for {
		// 检查自己是否还是leader
		if rf.getRole() != LEADER {
			rf.mu.Lock()
			DPrintf("[%d] 从leader退位,当前选期[%d]", rf.me, rf.currentTerm)
			return
		}

		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()

		// 开始AppendEntries
		select {
		case <-rf.closeCh:
			rf.mu.Lock()
			return
		default:
			rf.mu.Lock()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int) {
					var reply AppendEntriesReply
					if ok := rf.sendAppendEntries(i, &args, &reply); ok {
						// 会出现并发
						// 一把大锁保平安
						rf.mu.Lock()
						if rf.getRole() == LEADER && reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							DPrintf("[%d] leader发现选期更大的节点,转变为follower", rf.me)
							rf.setRole(FOLLOWER)
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		closeCh:   make(chan struct{}),

		lastHeartbeat: time.Now(),

		currentTerm: 0,
		votedFor:    -1,
		role:        FOLLOWER,
	}

	// Your initialization code here (3A, 3B, 3C).

	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
