package raft

import (
	"time"
)

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER

	HBINTERVAL      = 100 * time.Millisecond
	FOLLOWERTIMEOUT = 3000 * time.Millisecond

	LOGTHRESHOLD = 1024
)

type ApplyMsg struct {
	Index   int
	Command interface{}
}

type LogEntry struct {
	LogIndex   int
	LogTerm    int
	LogCommand LogCmd
	//LogCommand interface{}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogTerm   int
	PreLogIndex  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

type LogCmd struct {
	Cmd   string
	Key   []byte
	Value []byte
}

//provide interface to client
const (
	PUT = "PUT"
	DEL = "DEL"
	GET = "GET"
)

type GetArgs struct {
	Key []byte
}

type GetReply struct {
	Ok    bool
	Value []byte
}

type PutArgs struct {
	Key             []byte
	Value           []byte
	TimeoutInterval int
}

type PutReply struct {
	Ok bool
}

type DelArgs struct {
	Key             []byte
	TimeoutInterval int
}

type DelReply struct {
	Ok bool
}
