package raft

import (
	"fmt"
	"sort"
	"time"
)

const (
	EmptyTerm     = 0
	EmptyLogIndex = 0
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

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d T%d, Prev:[%d]T%d, Entries:(%d, %d], Commit:%d", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Success:%t, Conflict [%d]T%d", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, AppendEntries, Prev=[%d]T%d, Commit: %d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	rf.becomeFollowerLocked(args.Term)

	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "Follower log=%v", rf.LogString())
		}
	}()

	if len(rf.log) <= args.PrevLogIndex {
		LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: Reject Log, PrevLogIndex %d out of range", args.PrevLogIndex)
		return
	} // prevent out of bound

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog, "AppendEntries: Reject Log, PrevLogTerm %d != log[%d].Term %d", args.PrevLogTerm, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term)
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persistLocked()
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

	if len(rf.log) <= args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = EmptyTerm
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = rf.findFirstIndexWithTerm(args.PrevLogTerm)
		return
	}

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

		LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, AppendEntries, Reply:%s", peer, reply.String())

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if !reply.Success {
			prevNextIndex := rf.nextIndex[peer]
			var targetIndex int
			if reply.ConflictTerm == EmptyTerm {
				targetIndex = reply.ConflictIndex
			} else {
				firstTermIndex := rf.findFirstIndexWithTerm(reply.ConflictTerm)
				if firstTermIndex == EmptyLogIndex {
					targetIndex = reply.ConflictIndex
				} else {
					targetIndex = firstTermIndex
				}
			}

			if targetIndex < 1 {
				targetIndex = 1
			}

			if targetIndex < prevNextIndex {
				rf.nextIndex[peer] = targetIndex
			}

			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := rf.log[nextPrevIndex].Term

			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d", peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "Leader log: %s", rf.LogString())
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

		copyEntries := make([]LogEntry, len(rf.log[prevIdx+1:]))
		copy(copyEntries, rf.log[prevIdx+1:])

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,

			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      copyEntries,
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AppendEntries, Args:%s", peer, args.String())
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

func (rf *Raft) findFirstIndexWithTerm(term int) int {
	// L := LastIndex of LastTerm
	// R := FirstIndex of ThisTerm
	// L always equals to R - 1 when loop ends
	L := 0
	R := len(rf.log)
	check := func(idx int) bool {
		return rf.log[idx].Term >= term
	}
	for L+1 < R {
		mid := (L + R + 1) / 2
		if check(mid) {
			R = mid
		} else {
			L = mid
		}
	}
	if R >= len(rf.log) || rf.log[R].Term != term {
		return EmptyLogIndex
	}
	return R
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
