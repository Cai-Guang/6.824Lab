package raft

import (
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool
	Command      interface{}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: received log from leader [%d]T:%d, Len()=%d", args.LeaderId, args.PrevLogTerm, len(args.Entries))

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: term %d < currentTerm %d, append entries denied", args.Term, rf.currentTerm)
		return
	}

	rf.becomeFollowerLocked(args.Term)

	if len(rf.log) <= args.PrevLogIndex {
		LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: Reject Log, PrevLogIndex %d out of range", args.PrevLogIndex)
		return
	} // prevent out of bound

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: Reject Log, PrevLogTerm %d != log[%d].Term %d", args.PrevLogTerm, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term)
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: term %d >= currentTerm %d, append entries granted", args.Term, rf.currentTerm)
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex >= len(rf.log) {
			rf.commitIndex = len(rf.log) - 1
		}
		rf.applyCond.Signal()
	}

	// if len(rf.log) <= args.PrevLogIndex {
	// 	reply.ConflictIndex = len(rf.log)
	// 	reply.ConflictTerm = rf.log[len(rf.log)-1].Term
	// 	return
	// }

	// if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	reply.ConflictIndex = args.PrevLogIndex
	// 	reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
	// 	return
	// }

	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startReplication(term int) bool {
	rf.mu.Lock()

	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "context lost, leader %d, term %d, stop replication", rf.me, term)
		return false
	}

	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			LOG(rf.me, rf.currentTerm, DError, "failed to send append entries to peer %d", peer)
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if !reply.Success {
			idx := rf.nextIndex[peer] - 1
			term := rf.log[idx].Term

			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}

			// HIDDEN : binary search
			// L := 0
			// R := idx
			// check := func(idx int) bool {
			// 	return rf.log[idx].Term == term
			// }
			// for L+1 < R {
			// 	mid := (L + R + 1) / 2
			// 	if check(mid) {
			// 		R = mid
			// 	} else {
			// 		L = mid
			// 	}
			// }
			// idx = R

			rf.nextIndex[peer] = idx + 1

			LOG(rf.me, rf.currentTerm, DLog, "replication failed, next index %d, peer %d", idx+1, peer)

			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		majorityMatched := rf.getMajorityMatchedIndexLocked()

		if majorityMatched > rf.commitIndex {
			if rf.log[majorityMatched].Term == rf.currentTerm {
				LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
				rf.commitIndex = majorityMatched
				rf.applyCond.Signal()
			}
		}

		LOG(rf.me, rf.currentTerm, DLog, "replication successful, match index %d, next index %d, peer %d", rf.matchIndex[peer], rf.nextIndex[peer], peer)
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.nextIndex[peer] = len(rf.log)
			rf.matchIndex[peer] = len(rf.log) - 1
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,

			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
			LeaderCommit: rf.commitIndex,
		}

		LOG(rf.me, rf.currentTerm, DLog, "start replication to peer %d, prev index %d, prev term %d, entriesLength %d", peer, prevIdx, prevTerm, len(args.Entries))
		go replicateToPeer(peer, args)
	}

	return true
}

func (rf *Raft) getMajorityMatchedIndexLocked() int {
	// TODO : use priority queue
	majority := (len(rf.peers) - 1) / 2
	matched := make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		matched[i] = rf.matchIndex[i]
	}
	sort.Ints(matched)
	LOG(rf.me, rf.currentTerm, DLog, "majority matched index %d", matched[majority])
	return matched[majority]
}

func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		// rf.mu.Lock()

		ok := rf.startReplication(term)

		// rf.mu.Unlock()

		// deadlock
		if !ok {
			break
		}

		time.Sleep(replicationInterval)
	}
}
