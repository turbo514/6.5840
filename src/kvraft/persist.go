package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	return w.Bytes()
}

func (kv *KVServer) readPersist(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	data := make(map[string]string)
	if d.Decode(&data) != nil {
		panic("[%d] 从快照持久化恢复失败")
	} else {
		kv.data = data
	}
}
