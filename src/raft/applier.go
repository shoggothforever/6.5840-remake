package raft

import "time"

func (rf *Raft) apply() {
	for !rf.killed() {
		rf.lock()
		oldApply := rf.lastApplied
		oldCommit := rf.commitIndex

		if oldApply < rf.lastIndex {
			rf.lastApplied = rf.lastIndex
			rf.commitIndex = rf.lastIndex
			rf.unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}
		if oldCommit < rf.lastIndex {

			rf.commitIndex = rf.lastIndex
			rf.unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}

		if oldApply == oldCommit || (oldCommit-oldApply) >= len(rf.logs) {
			rf.unlock()
			time.Sleep(time.Millisecond * 5)
			continue
		}
		entry := make([]Log, oldCommit-oldApply)

		copy(entry, rf.logs[oldApply+1-rf.lastIndex:oldCommit+1-rf.lastIndex])

		rf.unlock()
		for key, value := range entry {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				CommandIndex: key + oldApply + 1,
				Command:      value.Command,
			}
		}

		rf.lock()
		if rf.lastApplied < oldCommit {
			rf.lastApplied = oldCommit
		}
		if rf.lastApplied > rf.commitIndex {
			rf.commitIndex = rf.lastApplied
		}

		rf.unlock()
		time.Sleep(time.Millisecond * 30)
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
