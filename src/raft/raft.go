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

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	DPrintf("[persist node%d] write [votedFor:%d term:%d lastIndex:%d lastTerm:%d logs:%+v", rf.me, rf.term, rf.votedFor, rf.lastIndex, rf.lastTerm, rf.logs)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.persister.snapshot)

}

func (rf *Raft) persistd(data []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.term)
	e.Encode(rf.lastIndex)
	e.Encode(rf.lastTerm)
	e.Encode(rf.logs)
	DPrintf("[persist node%d] write [votedFor:%d term:%d lastIndex:%d lastTerm:%d logs:%+v", rf.me, rf.term, rf.votedFor, rf.lastIndex, rf.lastTerm, rf.logs)
	raftState := w.Bytes()
	rf.persister.Save(raftState, data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.lock()
	defer rf.unlock()
	if d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.term) != nil ||
		d.Decode(&rf.lastIndex) != nil ||
		d.Decode(&rf.lastTerm) != nil ||
		d.Decode(&rf.logs) != nil {
		log.Fatal("decode data from persister failed")
	} else {
		DPrintf("[readPersist node%d ] read [votedFor:%d term:%d lastIndex:%d lastTerm:%d logs:%+v", rf.me, rf.votedFor, rf.term, rf.lastIndex, rf.lastTerm, rf.logs)
	}

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
