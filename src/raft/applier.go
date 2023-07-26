package raft

import "time"

func (rf *Raft) apply() {
	for !rf.killed() {
		rf.mu.Lock()
		oldApply := rf.lastApplied
		oldCommit := rf.commitIndex

		//after crash
		if oldApply < rf.lastIndex {
			rf.lastApplied = rf.lastIndex
			rf.commitIndex = rf.lastIndex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}
		if oldCommit < rf.lastIndex {

			rf.commitIndex = rf.lastIndex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}

		if oldApply == oldCommit || (oldCommit-oldApply) >= len(rf.logs) {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 5)
			continue
		}
		entry := make([]Log, oldCommit-oldApply)

		copy(entry, rf.logs[oldApply+1-rf.lastIndex:oldCommit+1-rf.lastIndex])

		rf.mu.Unlock()
		for key, value := range entry {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				CommandIndex: key + oldApply + 1,
				Command:      value.Command,
			}
		}

		rf.mu.Lock()
		if rf.lastApplied < oldCommit {
			rf.lastApplied = oldCommit
		}
		if rf.lastApplied > rf.commitIndex {
			rf.commitIndex = rf.lastApplied
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 30)
	}
}
