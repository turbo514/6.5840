package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const (
	FOLLOWER  int32 = 0
	CANDIDATE int32 = 1
	LEADER    int32 = 2
)

func (rf *Raft) setRole(role int32) {
	rf.role = role
}

func (rf *Raft) getRole() int32 {
	return rf.role
}
