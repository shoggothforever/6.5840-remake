package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotRes struct {
	Term int
}

func (rf *Raft) CallInstallSnapshot(server int, args *InstallSnapshotArgs, res *InstallSnapshotRes) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, res)
	if !ok {
		return
	}
	rf.lock()
	defer rf.unlock()
	if res.Term > rf.term {
		rf.term = res.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	} else {
		if rf.matchIndex[server] < args.LastIncludedIndex {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.CheckCommit()
		}
		if rf.nextIndex[server] < args.LastIncludedTerm+1 {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lock()
	defer rf.unlock()
	if index <= rf.lastIndex || index > rf.commitIndex {
		return
	}
	cnt := 1
	oldIndex := rf.lastIndex
	//找到可以作为快照存储的日志条目集合
	for k, v := range rf.logs {
		if k == 0 {
			continue
		}
		cnt++
		rf.lastIndex = k + oldIndex
		rf.lastTerm = v.Term
		if k+oldIndex == index {
			break
		}
	}
	newlog := make([]Log, 1)
	newlog = append(newlog, rf.logs[cnt:]...)
	rf.logs = newlog
	rf.persistd(snapshot)
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, res *InstallSnapshotRes) {
	rf.lock()
	defer rf.unlock()
	res.Term = rf.term
	if args.Term < rf.term {
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}
	if args.LastIncludedIndex <= rf.lastIndex {
		return
	}
	rf.resetClock()
	logs := make([]Log, 1)
	for i := args.LastIncludedIndex + 1; i < rf.getLastIndex(); i++ {
		logs = append(logs, rf.logs[i-rf.lastIndex])
	}
	rf.lastIndex = args.LastIncludedIndex
	rf.lastTerm = args.LastIncludedTerm
	rf.logs = logs
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.persistd(args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastTerm,
		SnapshotIndex: rf.lastIndex,
	}
	go func() { rf.applyChan <- msg }()
}
