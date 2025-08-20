package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const Eebug = false

func EPrintf(format string, a ...interface{}) (n int, err error) {
	if Eebug {
		log.Printf(format, a...)
	}
	return
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
