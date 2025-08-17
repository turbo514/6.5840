package raft

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	Entries          []Entry
	LastIncludeIndex int
}

func newLog(lastIncludedIndex, lastIncludeTerm int, entries []Entry) Log {
	return Log{
		Entries: append([]Entry{
			{Term: lastIncludeTerm},
		}, entries...),
		LastIncludeIndex: lastIncludedIndex,
	}
}

// 获取数组中索引为index的日志索引
func (rf *Raft) getLogIndex(index int) int {
	return rf.log.LastIncludeIndex + index
}

// 获取最新的日志的索引
func (rf *Raft) getLastLogIndex() int {
	return rf.getLogIndex(len(rf.log.Entries) - 1)
}

func (rf *Raft) getLastIncludeIndex() int {
	return rf.log.LastIncludeIndex
}

func (rf *Raft) getLastIncludeTerm() int {
	return rf.log.Entries[0].Term
}

// 获取日志索引为index的日志的数组索引
func (rf *Raft) getIndex(index int) int {
	return index - rf.log.LastIncludeIndex
}

// 获取日志索引为index的日志的Term
// 不考虑不存在的情况(或者说会直接panic)
func (rf *Raft) getLogTerm(index int) int {
	return rf.log.Entries[index-rf.log.LastIncludeIndex].Term
}

// 获取最新的日志的Term
func (rf *Raft) getLastLogTerm() int {
	return rf.log.Entries[len(rf.log.Entries)-1].Term
}

// 节点添加日志
func (rf *Raft) appendEntries(term int, command interface{}) {
	rf.log.Entries = append(rf.log.Entries, Entry{Term: term, Command: command})
}

// 获取日志索引index及往后的所有日志
func (rf *Raft) getAppendEntries(index int) []Entry {
	return rf.log.Entries[index-rf.log.LastIncludeIndex:]
}
