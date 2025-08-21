package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

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
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= int(0.8*float64(kv.maxraftstate))
}
