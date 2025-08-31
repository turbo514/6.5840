package shardkv

import (
	"crypto/rand"
	"log"
	"math/big"

	"6.5840/shardctrler"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 哈希函数,用于确定key所属的分片组
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func getMax(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}
