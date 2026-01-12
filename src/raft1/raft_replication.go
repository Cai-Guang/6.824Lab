package raft

import "time"

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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

			for idx > 0 && rf.log[idx].Term == term { // TODO : binary search
				idx--
			}

			rf.nextIndex[peer] = idx + 1

			LOG(rf.me, rf.currentTerm, DLog, "replication failed, next index %d, peer %d", idx+1, peer)

			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

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
