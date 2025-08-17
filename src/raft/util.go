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

const Eebug = false

func EPrintf(format string, a ...interface{}) {
	if Eebug {
		log.Printf(format, a...)
	}
}

const Febug = false

func FPrintf(format string, a ...interface{}) {
	if Febug {
		log.Printf(format, a...)
	}
}

const Gebug = false

func GPrintf(format string, a ...interface{}) {
	if Gebug {
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

// func dumpStacks() {
// 	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
// }

// func (rf *Raft) serLastApplied(index int32) {
// 	atomic.StoreInt32(&rf.lastApplied, index)
// }

// func (rf *Raft) getLastApplied() int32 {
// 	atomic.LoadInt32(&rf.lastApplied)
// }
