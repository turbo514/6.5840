package shardkv

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

type SnapshotStatus struct {
	LastApplied int
	Shards      map[int]*Shard
	SeqMap      map[int64]int64
	//History       map[int64]string
	LastConfig    shardctrler.Config
	CurrentConfig shardctrler.Config
}

func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= int(0.9*float64(kv.maxraftstate))
}

func (kv *ShardKV) makeSnapshot(lastApplied int) {
	//fmt.Println("制作了快照,index=", lastApplied)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshotStatus := SnapshotStatus{
		LastApplied: lastApplied,
		Shards:      kv.shards,
		SeqMap:      kv.seqMap,
		//History:       kv.history,
		LastConfig:    kv.lastConfig,
		CurrentConfig: kv.currentConfig,
	}
	e.Encode(snapshotStatus)
	kv.rf.Snapshot(lastApplied, w.Bytes())
}

func (kv *ShardKV) readPersist(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var status SnapshotStatus
	if err := d.Decode(&status); err != nil {
		panic(fmt.Errorf("从快照持久化恢复失败,err=%w", err))
	} else {
		kv.snapshotIndex = status.LastApplied
		kv.shards = status.Shards
		kv.seqMap = status.SeqMap
		//kv.history = status.History
		kv.lastConfig = status.LastConfig
		kv.currentConfig = status.CurrentConfig
	}
}
