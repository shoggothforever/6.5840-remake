package raft

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
	if args.Term < rf.currentTerm {
		reply.Granted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastIndex()) {
		reply.Granted = false
		reply.Term = rf.currentTerm
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.state = Follower
			rf.persist()
		}
		return
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		rf.state = Follower
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Granted = true
		reply.Term = rf.currentTerm
		rf.resetClock()
		rf.persist()
	}
}
