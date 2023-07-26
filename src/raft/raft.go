package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"log"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	clockTime time.Time
	term      int
	state     int
	votedFor  int

	logs       []Log
	matchIndex []int
	nextIndex  []int

	applyChan   chan ApplyMsg
	commitIndex int
	lastApplied int
	lastIndex   int
	lastTerm    int
}

func (rf *Raft) lock() {
	rf.mu.Lock()
}
func (rf *Raft) unlock() {
	rf.mu.Unlock()
}
func (rf *Raft) resetClock() {
	rf.clockTime = time.Now()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.lock()
	defer rf.unlock()
	term = rf.term
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.term)
	e.Encode(rf.lastIndex)
	e.Encode(rf.lastTerm)
	e.Encode(rf.logs)
	DPrintf("persister write [votedFor:%d term:%d lastIndex:%d lastTerm:%d logs:%+v", rf.votedFor, rf.term, rf.lastIndex, rf.lastTerm, rf.logs)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.persister.snapshot)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.lock()
	defer rf.unlock()
	if d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.term) != nil ||
		d.Decode(&rf.lastIndex) != nil ||
		d.Decode(&rf.lastTerm) != nil ||
		d.Decode(&rf.logs) != nil {
		log.Fatal("decode data from persister failed")
	} else {
		DPrintf("persister read [votedFor:%d term:%d lastIndex:%d lastTerm:%d logs:%+v", rf.votedFor, rf.term, rf.lastIndex, rf.lastTerm, rf.logs)
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	if !ok {
		return ok
	}
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
	if rf.killed() {
		return
	}
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

	n := len(rf.peers)
	for i := 0; i < n; i++ {
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

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	go rf.apply()
	for rf.killed() == false {
		rf.lock()
		// Your code here (2A)
		// Check if a leader election should be started.
		if time.Since(rf.clockTime) > time.Duration(ElectTimeoutBase+rand.Intn(ElectTimeoutRange))*time.Millisecond && rf.state != Leader {
			DPrintf("[ticker node%d term:%d]timeout", rf.me, rf.term)
			go rf.election()
		}

		if rf.state == Leader {
			n := len(rf.peers)
			ri := rf.getLastIndex()
			rt := rf.getLastLogTerm()
			rf.CheckCommit()
			for i := 0; i < n; i++ {
				if i == rf.me {
					continue
				}
				req := AppendEntriesReq{
					Term:         rf.term,
					LeaderID:     rf.me,
					PrevLogIndex: ri,
					PrevLogTerm:  rt,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				//DPrintf("[leader[%d] SendHeartBeat to %d node\n", rf.me, i)
				//TODO:check nextindex and matchindex
				if (rf.nextIndex[i] <= ri || rf.nextIndex[i]-rf.matchIndex[i] != 1) && ri != 0 {
					if rf.matchIndex[i] < rf.lastIndex {
						//TODO:install snapshot
					} else {
						req.LogEntries = make([]Log, rf.getLastIndex()-rf.matchIndex[i])
						copy(req.LogEntries, rf.logs[rf.matchIndex[i]+1-rf.lastIndex:])
						req.PrevLogIndex = rf.matchIndex[i]
						req.PrevLogTerm = rf.getLogTerm(rf.matchIndex[i])
						go rf.SyncLog(i, &req, &reply)
					}
				} else {
					go rf.SendHeartBeat(i, &req, &reply)
				}
			}
		}
		rf.unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.resetClock()
	rf.term = 0
	rf.votedFor = -1
	rf.state = Follower
	//rf.lastIndex=0
	//rf.lastTerm=0
	rf.applyChan = applyCh
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	log := Log{
		Term: 0,
	}
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, log)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
