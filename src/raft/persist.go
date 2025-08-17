package raft

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	FPrintf("[%d] 持久化中,currentTerm=%d,votedFor=%d,log=%+v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log)

	raftstate := rf.encodeState()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, snapshot)

	FPrintf("[%d] 持久化完成", rf.me)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	FPrintf("[%d] 从持久化恢复", rf.me)

	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("节点[%d] 恢复持久化失败\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}

	rf.lastApplied = rf.getLastIncludeIndex()
	rf.commitIndex = rf.getLastIncludeIndex()

	FPrintf("[%d] 持久化恢复结果:currentTerm=%d,votedFor=%d,log=%+v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	GPrintf("[%d] 开始创建快照,index=%d", rf.me, index)

	select {
	case <-rf.closeCh:
		return
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 判断是否是过期的快照
	if index <= rf.getLastIncludeIndex() || index > rf.commitIndex {
		GPrintf("[%d] 过期快照,index=%d,lastIncludeIndex=%d,commitIndex=%d",
			rf.me, index, rf.getLastIncludeIndex(), rf.commitIndex)
		return
	}

	arrayIndex := rf.getIndex(index)
	lastIncludeTerm := rf.log.Entries[arrayIndex].Term

	// 日志压缩和更新
	rf.log = newLog(index, lastIncludeTerm, rf.log.Entries[arrayIndex+1:])

	rf.persister.Save(rf.encodeState(), snapshot)
	GPrintf("[%d] 创建快照完成,index=%d", rf.me, index)
}
