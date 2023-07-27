package raft

func (rf *Raft) HeartBeat(args *AppendEntriesReq, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//HINT:判断是否遵守基本规则
	DPrintf("[HeartBeat node%d term:%d]received HeartBeatCheck from leader:%d term:%d currentLogs:%+v", rf.me, args.Term, args.LeaderID, rf.term, rf.logs)
	reply.Success = false
	if args.Term < rf.term {
		reply.Term = rf.term
		return
	} else if args.Term > rf.term {
		reply.Term = args.Term
		rf.term = args.Term
		rf.resetClock()
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	} else {
		rf.resetClock()
		rf.state = Follower
		rf.persist()
	}

	//HINT:进行日志匹配检测 根据  pi 和 pt
	ri := rf.getLastIndex()
	rt := rf.getLastLogTerm()
	pi := args.PrevLogIndex
	pt := args.PrevLogTerm
	//insight:当前节点拥有日志少于leader,直接传回当前节点的最后索引号
	if ri < pi {
		reply.Index = ri
		return
	}
	/*
		insight:当前节点的提交日志即使包括了传来的日志依旧新于leader,表明已经做过快照处理了,对日志进行压缩处理,
		之后在syncLog中会比对当前节点与leader节点的日志压缩程度,再做进一步处理
	*/
	//Hint:check snapshot

	if rf.lastIndex > pi {
		if rf.lastIndex >= pi+len(args.LogEntries) {
			reply.Index = rf.lastIndex
			return
		}
		args.PrevLogTerm = args.LogEntries[rf.lastIndex-pi-1].Term
		args.LogEntries = args.LogEntries[rf.lastIndex-pi:]
		args.PrevLogIndex = rf.lastIndex
	}
	//insight:相同日志索引任期不同,回退
	if pt != rf.getLogTerm(pi) {
		reply.Index = rf.lastApplied
		if reply.Index > rf.lastIndex {
			reply.Index = rf.lastIndex
		}
		if reply.Index > pi-1 {
			reply.Index = pi - 1
		}
		return
	}
	reply.Success = true

	if ri == pi && rt == pt {
		if args.LeaderCommit > rf.commitIndex {
			tpi := ri
			if tpi > args.LeaderCommit {
				tpi = args.LeaderCommit
			}
			rf.commitIndex = tpi
		}
	}
	//HINT:just heart beat
	if len(args.LogEntries) == 0 {
		reply.Term = args.Term
		return
	}
	//HINT:totally match
	if ri >= pi+len(args.LogEntries) &&
		rf.getLogTerm(pi+len(args.LogEntries)) == args.LogEntries[len(args.LogEntries)-1].Term {
		reply.Term = args.Term
		return
	}
	next := pi + 1
	//for next <= ri && next-pi-1 < len(args.LogEntries) {
	//	DPrintf("sleep")
	//	break
	//}
	if next-pi-1 >= len(args.LogEntries) {
		return
	}
	rf.logs = rf.logs[:next-rf.lastIndex]
	rf.logs = append(rf.logs, args.LogEntries...)
	if args.LeaderCommit > rf.commitIndex {
		tri := ri
		if tri > args.LeaderCommit {
			tri = args.LeaderCommit
		}
		rf.commitIndex = tri
	}
	rf.persist()
}
func (rf *Raft) SendHeartBeat(server int, args *AppendEntriesReq, reply *AppendEntriesReply) {
	//DPrintf("[HeartBeat node%d term:%d]send HeartBeatCheck to node:%d", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	//DPrintf("[HeartBeat node%d term:%d]receive HeartBeatCheck from node:%d reply:%+v", rf.me, args.Term, server, *reply)
	if !ok {
		return
	}
	rf.lock()
	defer rf.unlock()
	if rf.state != Leader {
		return
	}
	if !reply.Success {
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		} else {
			args.PrevLogIndex = reply.Index
			if args.PrevLogIndex < 0 {
				return
			}
			if args.PrevLogIndex < rf.lastIndex {
				snapargs := InstallSnapshotArgs{
					Term:              rf.term,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIndex,
					LastIncludedTerm:  rf.lastTerm,
					Data:              rf.persister.snapshot,
				}
				snapres := InstallSnapshotRes{}
				go rf.CallInstallSnapshot(server, &snapargs, &snapres)
				return
			} else {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.lastIndex].Term
				if args.PrevLogIndex == rf.lastIndex && rf.lastIndex != 0 {
					args.PrevLogTerm = rf.lastTerm
				}
				entry := make([]Log, rf.getLastIndex()-args.PrevLogIndex)
				copy(entry, rf.logs[args.PrevLogIndex-rf.lastIndex+1:])
				args.LogEntries = entry
				go rf.SyncLog(server, args, reply)
			}
		}
	} else {
		if rf.matchIndex[server] < args.PrevLogIndex {
			rf.matchIndex[server] = args.PrevLogIndex
			rf.CheckCommit()
		}
		if rf.nextIndex[server] < args.PrevLogIndex+len(args.LogEntries)+1 {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.LogEntries) + 1
		}
	}
}

func (rf *Raft) broadcast() {
	if rf.state == Leader {
		n := len(rf.peers)
		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] < rf.lastIndex {
				snapargs := InstallSnapshotArgs{
					Term:              rf.term,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIndex,
					LastIncludedTerm:  rf.lastTerm,
					Data:              rf.persister.snapshot,
				}
				snapres := InstallSnapshotRes{}
				go rf.CallInstallSnapshot(i, &snapargs, &snapres)

			} else {
				entry := make([]Log, rf.getLastIndex()-rf.matchIndex[i])
				copy(entry, rf.logs[rf.matchIndex[i]+1-rf.lastIndex:])
				req := AppendEntriesReq{
					Term:         rf.term,
					LeaderID:     rf.me,
					PrevLogIndex: rf.getLastIndex(),
					PrevLogTerm:  rf.getLogTerm(rf.matchIndex[i]),
					LeaderCommit: rf.commitIndex,
					LogEntries:   entry,
				}
				reply := AppendEntriesReply{}
				go rf.SyncLog(i, &req, &reply)
			}
		}
	}
}
