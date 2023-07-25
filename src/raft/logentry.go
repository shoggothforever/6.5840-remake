package raft

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

func (rf *Raft) getLastIndex() int {
	return rf.lastIndex + len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) == 1 {
		return rf.lastTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}
func (rf *Raft) getLogTerm(index int) int {
	if index > rf.lastIndex {
		return rf.logs[index-rf.lastIndex].Term
	}
	return rf.lastTerm
}

type ByKey []int

func (b ByKey) Len() int {
	return len(b)
}
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByKey) Less(i, j int) bool { return b[i] < b[j] }
