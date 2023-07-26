package raft

import (
	"math/rand"
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

func (rf *Raft) ticker() {
	go rf.apply()
	for rf.killed() == false {
		rf.lock()
		// Your code here (2A)
		// Check if a leader election should be started.
		if time.Since(rf.clockTime) > time.Duration(ElectTimeoutBase+rand.Intn(ElectTimeoutRange))*time.Millisecond && rf.state != Leader {
			DPrintf("[ticker node%d term:%d]start to election", rf.me, rf.term)
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
					LogEntries:   nil,
				}
				reply := AppendEntriesReply{}
				//DPrintf("[leader[%d] SendHeartBeat to %d node\n", rf.me, i)
				if (rf.nextIndex[i] <= ri || rf.nextIndex[i]-rf.matchIndex[i] != 1) && ri != 0 {
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
		ms := 30 + (rand.Int63() % 30)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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

	//记录当前任期
	curTerm := rf.term
	rf.unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var reply RequestVoteReply
		go rf.sendRequestVote(i, &req, &reply, &voted, &granted)
	}

	for {
		time.Sleep(WaitForVote * time.Millisecond)
		rf.lock()
		//DPrintf("[election node%d term:%d]current voted:%d granted:%d", rf.me, rf.term, voted, granted)
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
