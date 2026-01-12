package raft

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "can't become follower, lower term T:%d", term)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s became follower, T:%d->T%d", rf.role, rf.currentTerm, term)

	rf.role = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become candidate, already leader")
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s became candidate, T:%d->T%d", rf.role, rf.currentTerm, rf.currentTerm)

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "%s can't become leader, already %s", rf.role, rf.role)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s became leader, T:%d->T%d", rf.role, rf.currentTerm, rf.currentTerm)

	rf.role = Leader
}

func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return rf.role != role || rf.currentTerm != term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}
