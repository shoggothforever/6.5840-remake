package raft

import "sort"

type Log struct {
	Command interface{}
	Term    int
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

type AppendEntriesReq struct {
	//只有领导者有权调用AppendEntriesRPC
	//领导者任期
	Term int
	//领导者的me属性
	LeaderID int
	//针对目标跟随者的日志匹配属性（索引号和任期号，需要做相应的优化）
	PrevLogIndex int
	PrevLogTerm  int
	//领导者已经提交的最新日志编号
	LeaderCommit int
	//需要补充给跟随者的日志条目
	LogEntries []Log
}

type AppendEntriesReply struct {
	//如果检测到大于Args.Term则领导者需要退位
	Term int
	//如果跟随者的日志与prevLogIndex 和 prevLogTerm 匹配则返回true
	Success bool
	//表明日志不匹配，需要对nextIndex进行修正
	Index int
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	// Your code here (2B).
	rf.lock()
	defer rf.unlock()
	term = rf.term
	if rf.state != Leader {
		isLeader = false
		return
	}
	log := Log{
		Command: command,
		Term:    rf.term,
		//Index:   rf.getLastIndex() + 1,
	}
	rf.logs = append(rf.logs, log)
	rf.persist()
	rf.matchIndex[rf.me] = len(rf.logs) - 1 + rf.lastIndex
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= rf.lastIndex {
			entry := make([]Log, rf.getLastIndex()-rf.matchIndex[i])

			copy(entry, rf.logs[rf.matchIndex[i]+1-rf.lastIndex:])
			args := AppendEntriesReq{
				Term:         rf.term,
				LeaderID:     rf.me,
				PrevLogIndex: rf.matchIndex[i],
				PrevLogTerm:  rf.getLogTerm(rf.matchIndex[i]),
				LeaderCommit: rf.commitIndex,
				LogEntries:   entry,
			}
			reply := AppendEntriesReply{}
			go rf.SyncLog(i, &args, &reply)
		}
	}
	index = len(rf.logs) - 1 + rf.lastIndex
	term = log.Term
	DPrintf("[ Start node:%d term:%d] append logEntry index:%d term:%d isLeader:%+v\n"+
		"next:%+v,match:%+v\n"+
		"cid:%d,aid:%d,logs:%+v", rf.me, rf.term, index, term, isLeader, rf.nextIndex, rf.matchIndex, rf.commitIndex, rf.lastApplied, rf.logs)
	return
}

func (rf *Raft) SyncLog(server int, args *AppendEntriesReq, reply *AppendEntriesReply) {
	for !rf.killed() {
		DPrintf("[SyncLog server:%d term:%d]send AppendEntriesReq [to node%d] [args:%+v]\n", rf.me, rf.term, server, *args)
		ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
		DPrintf("[SyncLog server:%d term:%d]receive AppendEntriesRes [from node%d ][res:%+v]\n", rf.me, rf.term, server, *reply)
		if !ok {
			return
		}
		rf.lock()
		if rf.state != Leader {
			rf.unlock()
			return
		}
		if !reply.Success {
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				rf.unlock()
				return
			} else {
				args.PrevLogIndex = reply.Index
				if args.PrevLogIndex < 0 {
					rf.unlock()
					return
				}
				if args.PrevLogIndex-rf.lastIndex < 0 {
					x := make([]byte, len(rf.persister.snapshot))
					copy(x, rf.persister.snapshot)
					snapargs := InstallSnapshotArgs{
						Term:              rf.term,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIndex,
						LastIncludedTerm:  rf.lastTerm,
						Data:              x,
					}

					snapres := InstallSnapshotRes{}
					go rf.CallInstallSnapshot(server, &snapargs, &snapres)
					rf.unlock()
					return
				} else {
					args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
					entry := make([]Log, rf.getLastIndex()-args.PrevLogIndex)
					//copy(entry, rf.logs[args.PrevLogIndex+1:])
					copy(entry, rf.logs[args.PrevLogIndex-rf.lastIndex+1:])
					args.LogEntries = entry
				}
			}
			rf.unlock()
		} else {
			loglen := args.PrevLogIndex + len(args.LogEntries)
			if rf.matchIndex[server] < loglen {
				rf.matchIndex[server] = loglen
				rf.CheckCommit()
			}
			if rf.nextIndex[server] < loglen+1 {
				rf.nextIndex[server] = loglen + 1
			}
			rf.unlock()
			return
		}
	}
}

type ByKey []int

func (b ByKey) Len() int           { return len(b) }
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByKey) Less(i, j int) bool { return b[i] < b[j] }

func (rf *Raft) CheckCommit() {
	if rf.state != Leader {
		return
	}
	commit := make(ByKey, len(rf.peers))
	for k, v := range rf.matchIndex {
		commit[k] = v
	}
	sort.Sort(ByKey(commit))
	if commit[len(rf.peers)/2] >= rf.lastIndex &&
		rf.logs[commit[len(rf.peers)/2]-rf.lastIndex].Term == rf.term &&
		commit[len(rf.peers)/2] > rf.commitIndex {
		rf.commitIndex = commit[len(rf.peers)/2]
	}
}
