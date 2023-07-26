package raft

import "time"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock()
	defer rf.unlock()
	if args.Term < rf.term {
		DPrintf("[RequestVote node:%d term:%d]disagree candidate%d for term out", rf.me, rf.term, args.CandidateId)
		reply.Granted = false
		reply.Term = rf.term
		return
	}
	if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastIndex()) {
		DPrintf("[RequestVote node:%d term:%d]disagree candidate%d for logs dismatch", rf.me, rf.term, args.CandidateId)
		reply.Granted = false
		reply.Term = rf.term
		if args.Term > rf.term {
			rf.term = args.Term
			rf.state = Follower
			rf.persist()
		}
		return
	}
	if args.Term > rf.term ||
		(args.Term == rf.term &&
			(rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		DPrintf("[RequestVote node:%d term:%d]agree candidate%d ", rf.me, args.Term, args.CandidateId)
		rf.state = Follower
		rf.votedFor = args.CandidateId
		rf.term = args.Term
		reply.Granted = true
		reply.Term = rf.term
		rf.resetClock()
		rf.persist()
	}
}
func (rf *Raft) election() {
	rf.lock()
	//选举开始,任期＋1,变为候选人,为自己投票
	rf.state = Candidate
	rf.term += 1
	rf.votedFor = rf.me
	rf.persist()
	req := RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	//记录投票完成数以及支持数
	var voted, granted int = 1, 1
	var reply RequestVoteReply
	//记录当前任期
	curTerm := rf.term
	rf.unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &req, &reply, &voted, &granted)
	}

	for {
		time.Sleep(WaitForVote * time.Millisecond)
		rf.lock()
		DPrintf("[election node%d term:%d]current voted:%d granted:%d", rf.me, rf.term, voted, granted)
		if rf.state != Candidate || curTerm != rf.term {
			rf.unlock()
			return
		}
		if granted <= len(rf.peers)/2 {
			if granted+len(rf.peers)-voted <= len(rf.peers)/2 {
				rf.unlock()
				return
			}
		} else {
			rf.toLeader()
			rf.unlock()
			return
		}
		if voted == len(rf.peers) {
			rf.unlock()
			return
		}
		rf.unlock()

	}
}
