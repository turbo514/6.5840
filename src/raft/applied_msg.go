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
			// 有已提交但未应用的日志
			// for i, entry := range rf.log.entries[rf.getIndex(rf.commitIndex):] {
			// 	rf.applyCh <- ApplyMsg{
			// 		CommandValid: true,
			// 		Command:      entry.Command,
			// 		CommandIndex: rf.getLogIndex(i),
			// 	}
			// }

			// 有已提交但未应用的日志,开始应用
			// 这一版或许语义更清晰点
			// 不加锁是因为rf.commitIndex只会正向增长,应该没有关系?
			for {
				lastApplied := rf.lastApplied
				commitIndex := rf.getCommitIndex()

				if lastApplied >= commitIndex {
					break
				}

				index := rf.getIndex(int(lastApplied + 1))

				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log.entries[index].Command,
					CommandIndex: int(lastApplied + 1),
				}
				rf.applyCh <- msg

				rf.lastApplied++
				EPrintf("[%d] 应用了日志[%d]", rf.me, lastApplied+1)
			}
		}
	}
}

// 更新leader的CommitIndex
// FIXME: 真的需要锁吗?
func (rf *Raft) updateCommitIndex() {
	// 寻找当前大多数节点都大于等于的值
	minest := getMedian(rf.matchIndex)

	// 检查是否是当前任期提交的
	//EPrintf("[%d] 更新提交前,commitIndex=%d", rf.me, rf.commitIndex)
	if int32(minest) > rf.commitIndex && rf.getLogTerm(minest) == rf.currentTerm {
		rf.setCommitIndex(getMax32(int32(minest), rf.commitIndex))
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
	}
}
