package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	electionTimeoutMin      time.Duration = 250 * time.Millisecond
	electionTimeoutMax      time.Duration = 500 * time.Millisecond
	electionTimeoutInterval int64         = int64(electionTimeoutMax - electionTimeoutMin)

	replicationInterval time.Duration = 100 * time.Millisecond
)

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(electionTimeoutInterval))
}

func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) >= rf.electionTimeout
}

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role        Role
	currentTerm int
	votedFor    int // -1 if none

	electionStart   time.Time
	electionTimeout time.Duration

	log         []LogEntry
	nextIndex   []int // logical index
	matchIndex  []int
	commitIndex int

	lastApplied int
	applyCond   *sync.Cond
	applyCh     chan raftapi.ApplyMsg
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return index, term, false
	}

	index = len(rf.log)
	term = rf.currentTerm
	entry := LogEntry{Term: term, CommandValid: true, Command: command}

	rf.log = append(rf.log, entry)

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	LOG(rf.me, rf.currentTerm, DLog, "Leader %d, Term %d, Start command %v, Index %d", rf.me, rf.currentTerm, command, index)

	// go rf.startReplication(term)

	return index, term, isLeader
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

func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return rf.role != role || rf.currentTerm != term
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = append(rf.log, LogEntry{Term: 0, CommandValid: false, Command: nil})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = 0

	rf.lastApplied = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applyTicker()

	return rf
}

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		lastAppliedIndex := rf.lastApplied
		currentCommitIndex := rf.commitIndex

		entries := make([]LogEntry, 0)

		for i := lastAppliedIndex + 1; i <= currentCommitIndex; i++ {
			entries = append(entries, rf.log[i])
		}

		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: lastAppliedIndex + i + 1,
			}
		}
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "applied entries for [%d, %d], len = %d", lastAppliedIndex+1, currentCommitIndex+1, len(entries))
		rf.lastApplied = currentCommitIndex
		rf.mu.Unlock()
	}
}
