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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

	FPrintf("[%d] 持久化完成", rf.me)
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

	FPrintf("[%d] 持久化恢复结果:currentTerm=%d,votedFor=%d,log=%+v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}
