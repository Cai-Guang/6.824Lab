package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote Rejected, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote Rejected, Already voted for %d", args.CandidateId, rf.votedFor)
		return
	}

	if rf.isMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote Rejected, S%d's log less up-to-date", args.CandidateId, rf.me)
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote Granted", args.CandidateId)
}

func (rf *Raft) startElection(term int) bool {
	vote := 1

	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			LOG(rf.me, rf.currentTerm, DError, "failed to send request vote to peer %d", peer)
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "context lost, candidate %d, term %d", rf.me, term)
			return
		}

		if reply.VoteGranted {
			vote++
			if vote > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
				return
			}
		}
	}

	rf.mu.Lock()
	if rf.contextLostLocked(Candidate, term) {
		rf.mu.Unlock()
		LOG(rf.me, rf.currentTerm, DVote, "context lost, candidate %d, term %d", rf.me, term)
		return false
	}

	entriesLength := len(rf.log)
	lastTerm, lastIndex := rf.log[entriesLength-1].Term, entriesLength-1
	rf.mu.Unlock()

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:        term,
			CandidateId: rf.me,

			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}

		go askVoteFromPeer(peer, args)
	}

	// rf.mu.Lock()
	// rf.currentTerm = term
	// rf.mu.Unlock()
	return true
}

// compare last log of candidate and leader(self)
func (rf *Raft) isMoreUpToDate(candidateIndex, candidateTerm int) bool {
	entriesLength := len(rf.log)
	lastTerm, lastIndex := rf.log[entriesLength-1].Term, entriesLength-1
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log: Me: [%d]T:%d, Candidate :[%d]T:%d", lastIndex, lastTerm, candidateIndex, candidateTerm)

	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}

	return lastIndex > candidateIndex
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
