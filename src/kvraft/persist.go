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
		kv.snapshotIndex = status.LastApplied
	}
}

type SnapshotStatus struct {
	LastApplied int
	Data        map[string]string
	SeqMap      map[int64]int64
	History     map[int64]string
}

func (kv *KVServer) makeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshotStatus := SnapshotStatus{
		LastApplied: index,
		Data:        kv.data,
		SeqMap:      kv.seqMap,
		History:     kv.history,
	}
	e.Encode(snapshotStatus)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= int(0.9*float64(kv.maxraftstate))
}
