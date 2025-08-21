package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

func (kv *KVServer) readPersist(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var status SnapshotStatus
	if d.Decode(&status) != nil {
		panic("[%d] 从快照持久化恢复失败")
	} else {
		kv.data = status.Data
		kv.seqMap = status.SeqMap
		kv.history = status.History
		kv.lastApplied = status.LastApplied
	}
}
