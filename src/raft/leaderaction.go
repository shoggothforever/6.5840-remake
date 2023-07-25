package raft

import (
	"time"
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
	rf.state = Leader
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
}
func (rf *Raft) broadcast() {
	if rf.state == Leader {
		n := len(rf.peers)
		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}

		}
	}
}

func (rf *Raft) HeartBeat(args *AppendEntriesReq, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.clockTime = time.Now()
		rf.persist()
	} else {
		reply.Success = true
		rf.resetClock()
		rf.state = Follower
		rf.persist()
	}

}
func (rf *Raft) SendHeartBeat(server int, args *AppendEntriesReq, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	if !ok {
		return
	}
	rf.lock()
	defer rf.unlock()
	if rf.state != Leader {
		return
	}
	if !reply.Success {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		} //TODO: else {} installSnapShot RPC

	} else {
		if rf.matchIndex[server] < args.PrevLogIndex {
			rf.matchIndex[server] = args.PrevLogIndex
			//TODO rf.UpdateCommit()

		}
		if rf.nextIndex[server] < args.PrevLogIndex+len(args.LogEntries)+1 {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.LogEntries) + 1

		}
	}
}
