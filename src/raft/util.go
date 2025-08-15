package raft

import (
	"log"
	"sync/atomic"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const Eebug = true

func EPrintf(format string, a ...interface{}) {
	if Eebug {
		log.Printf(format, a...)
	}
}

const (
	FOLLOWER  int32 = 0
	CANDIDATE int32 = 1
	LEADER    int32 = 2
)

func (rf *Raft) setRole(role int32) {
	atomic.StoreInt32(&rf.role, role)
}

func (rf *Raft) getRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setCommitIndex(index int32) {
	atomic.StoreInt32(&rf.commitIndex, index)
}

func (rf *Raft) getCommitIndex() int32 {
	return atomic.LoadInt32(&rf.commitIndex)
}

// func (rf *Raft) serLastApplied(index int32) {
// 	atomic.StoreInt32(&rf.lastApplied, index)
// }

// func (rf *Raft) getLastApplied() int32 {
// 	atomic.LoadInt32(&rf.lastApplied)
// }
