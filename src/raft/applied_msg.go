package raft

import (
	"sort"
)

// 不断异步应用日志的线程
func (rf *Raft) applyMsgFunc() {

	for {
		select {
		case <-rf.closeCh:
			return

		case <-rf.applyNotifier: // 应用日志(其实就是把命令提交给应用层)
			// 有已提交但未应用的日志,开始应用
			// 这一版或许语义更清晰点

			for {
				rf.mu.Lock() // TODO: 或许可以改成先获取需要应用的日志,这样就不用不断获取锁了
				if rf.lastApplied >= rf.commitIndex {
					rf.mu.Unlock()
					break
				}

				rf.lastApplied++
				index := rf.getIndex(rf.lastApplied)

				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log.Entries[index].Command,
					CommandIndex: int(rf.lastApplied),
					CommandTerm:  rf.log.Entries[index].Term,
				}
				//log.Printf("[%d] 发送了ApplyMsg,Command=%+v\n", rf.me, msg.Command)
				rf.mu.Unlock()

				rf.applyCh <- msg

				EPrintf("[%d] 应用了日志[%d]", rf.me, rf.lastApplied+1)
			}
		}
	}
}

// 更新leader的CommitIndex
// FIXME: 真的需要锁吗?
func (rf *Raft) updateCommitIndex() {
	if rf.commitIndex >= rf.matchIndex[rf.me] {
		return
	}
	// 寻找当前大多数节点都大于等于的值
	minest := getMedian(rf.matchIndex)

	// 检查是否是当前任期提交的
	//EPrintf("[%d] 更新提交前,commitIndex=%d", rf.me, rf.commitIndex)
	if minest > rf.commitIndex && rf.getLogTerm(minest) == rf.currentTerm {
		rf.commitIndex = getMax(minest, rf.commitIndex)
		rf.signalApply()
	}
	//EPrintf("[%d] 更新提交后,commitIndex=%d", rf.me, rf.commitIndex)

	//rf.applyNotifier.Signal()
}

// 返回中位数
func getMedian(nums []int) int {
	tmp := make([]int, len(nums))
	copy(tmp, nums)

	sort.Ints(tmp)
	return tmp[(len(nums)-1)/2]
}

func (rf *Raft) signalApply() {
	select {
	case rf.applyNotifier <- struct{}{}:
	default: // 已经有信号了，就别重复发
		//fmt.Printf("[%d] applyNotifier已满,疑似阻塞\ns", rf.me)
	}
}
