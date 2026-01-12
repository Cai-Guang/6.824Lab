package raft

import "time"

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: term %d < currentTerm %d, append entries denied", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: term %d >= currentTerm %d, append entries granted", args.Term, rf.currentTerm)

	rf.becomeFollowerLocked(args.Term)
	reply.Term = args.Term
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
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}

		go replicateToPeer(peer, args)
	}

	return true
}

func (rf *Raft) replicationTicker(term int) {
	for rf.killed() == false {
		// rf.mu.Lock()

		ok := rf.startReplication(term)

		// rf.mu.Unlock()
		if !ok {
			break
		}

		time.Sleep(replicationInterval)
	}
}
