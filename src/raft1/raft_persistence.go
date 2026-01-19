package raft

import (
	"bytes"

	"6.5840/labgob"
)

func (rf *Raft) persistLocked() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	raftstate := w.Bytes()

	//

	rf.persister.Save(raftstate, nil)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	var currentTerm int
	var votedFor int
	var log []LogEntry

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currentTerm error: %v", err)
		return
	}
	rf.currentTerm = currentTerm

	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor error: %v", err)
		return
	}
	rf.votedFor = votedFor

	if err := d.Decode(&log); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error: %v", err)
		return
	}
	rf.log = log
	LOG(rf.me, rf.currentTerm, DPersist, "Read Persist %v", rf.role)
}
