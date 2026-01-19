package raft

import (
	"fmt"
	"log"
)

const DLog = "LOG"
const DError = "ERROR"
const DVote = "VOTE"
const DApply = "APPLY"
const DPersist = "PERSIST"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func LOG(me int, term int, topic string, format string, a ...interface{}) {
	DPrintf("%s: Raft %d [%d]: %s", topic, me, term, fmt.Sprintf(format, a...))
}
