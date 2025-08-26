package shardctrler

import (
	"crypto/rand"
	"log"
	"math/big"
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

func cloneGroups(origin map[int][]string) map[int][]string {
	newGroups := make(map[int][]string, len(origin)+1)
	for gid, server := range origin {
		cloneServer := make([]string, len(server))
		copy(cloneServer, server)
		newGroups[gid] = cloneServer
	}
	return newGroups
}

func copyShards(target, origin *[NShards]int) {
	*target = *origin
}

func (sc *ShardCtrler) getConfig(num int) *Config {
	return &sc.configs[num]
}

func (sc *ShardCtrler) getLastConfigNum() int {
	return sc.configs[len(sc.configs)-1].Num
}

func (sc *ShardCtrler) getLastConfig() *Config {
	return &sc.configs[len(sc.configs)-1]
}
