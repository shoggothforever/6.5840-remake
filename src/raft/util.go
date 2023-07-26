package raft

import (
	"flag"
	"log"
	"time"
)

// Debugging
// const Debug = true
var Debug = flag.Bool("p", false, "default setting false to avoid print debug info")

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if *Debug {
		log.SetFlags(log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
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
