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

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voted, granted *int) bool {
	DPrintf("[sendRequestVote server%d term:%d] send args to node%d [args:%+v]", rf.me, rf.term, server, *args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("[sendRequestVote server%d term:%d] receive reply from node%d [res:%+v]", rf.me, rf.term, server, *reply)
	rf.lock()
	defer rf.unlock()
	*voted += 1
	if reply.Granted {
		*granted += 1
	} else {
		if reply.Term > rf.term {
			rf.state = Follower
			rf.term = reply.Term
			rf.votedFor = -1
			rf.resetClock()
			rf.persist()
		}
	}
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock()
	defer rf.unlock()
	if args.Term < rf.term {
		//DPrintf("[RequestVote node:%d term:%d]disagree candidate%d for term out", rf.me, rf.term, args.CandidateId)
		reply.Granted = false
		reply.Term = rf.term
		return
	}
	if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastIndex()) {
		//DPrintf("[RequestVote node:%d term:%d]disagree candidate%d for logs dismatch", rf.me, rf.term, args.CandidateId)
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
		//DPrintf("[RequestVote node:%d term:%d]agree candidate%d ", rf.me, args.Term, args.CandidateId)
		rf.state = Follower
		rf.votedFor = args.CandidateId
		rf.term = args.Term
		reply.Granted = true
		reply.Term = rf.term
		rf.resetClock()
		rf.persist()
	}
}
