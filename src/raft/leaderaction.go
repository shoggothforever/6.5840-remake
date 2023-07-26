package raft

import (
	"sort"
)

const (
	Follower = iota
	Candidate
	Leader
)
const (
	ElectTimeoutBase  = 100
	ElectTimeoutRange = 300
	WaitForVote       = 10
)

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

type ExtendAppendReply struct {
	//
	//FixMatch bool
	XIndex int
	XTerm  int
	XLen   int
}
type AppendEntriesReply struct {
	//如果检测到大于Args.Term则领导者需要退位
	Term int
	//如果跟随者的日志与prevLogIndex 和 prevLogTerm 匹配则返回true
	Success bool
	//表明日志不匹配，需要对nextIndex进行修正
	Index int
}

func (rf *Raft) toLeader() {
	DPrintf("[node%d become leader at term:%d]", rf.me, rf.term)
	rf.state = Leader
	rf.persist()
	n := len(rf.peers)
	lastindex := rf.getLastIndex()
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastindex + 1
	}
	rf.matchIndex[rf.me] = lastindex
	rf.nextIndex[rf.me] = lastindex + 1
	rf.broadcast()
}
func (rf *Raft) broadcast() {
	if rf.state == Leader {
		n := len(rf.peers)
		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] < rf.lastIndex {
				//TODO install snapshot
			} else {
				entry := make([]Log, rf.getLastIndex()-rf.matchIndex[i])
				//copy(entry, rf.logs[rf.matchIndex[i]+1-rf.lastIndex:])
				copy(entry, rf.logs[rf.matchIndex[i]+1:])
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

func (rf *Raft) HeartBeat(args *AppendEntriesReq, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//HINT:判断是否遵守基本规则
	DPrintf("[HeartBeat node%d term:%d]received HeartBeatCheck from leader:%d term:%d", rf.me, args.Term, args.LeaderID, rf.term)
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
	for next <= ri && next-pi-1 < len(args.LogEntries) {
		DPrintf("sleep")
		break
	}
	if next-pi-1 >= len(args.LogEntries) {
		return
	}
	rf.logs = rf.logs[:next-rf.lastIndex]
	rf.logs = append(rf.logs, args.LogEntries[next-pi-1:]...)
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
	DPrintf("[HeartBeat node%d term:%d]send HeartBeatCheck to node:%d", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	DPrintf("[HeartBeat node%d term:%d]receive HeartBeatCheck from node:%d reply:%+v", rf.me, args.Term, server, *reply)
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
		} //TODO: else {} installSnapShot RPC

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
					//TODO: install snapshot RPC

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

func (b ByKey) Len() int {
	return len(b)
}
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
