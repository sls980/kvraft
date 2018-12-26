package raft

import (
	"bytes"
	"encoding/gob"
	"errors"

	//"fmt"
	"kvraft/config"
	"kvraft/persister"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

func MakeRaftServer(configFile string) {
	rf := &Raft{}
	rf.init(configFile)
	rf.startServer()
}

type Raft struct {
	//owner data
	mu             sync.Mutex
	peersEndPoints *config.Config
	persister      *persister.Persister

	state         int
	voteCount     int
	leaderId      int
	chanCommit    chan int
	chanHeartbeat chan int
	chanGrantVote chan int
	chanLeader    chan bool
	chanApply     chan ApplyMsg

	//raft data
	//persistent state on all server
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all server
	commitIndex int
	lastApplied int

	//volatile state on leader
	nextIndex  map[int]int
	matchIndex map[int]int
}

func (rf *Raft) init(configFile string) {
	rf.peersEndPoints = config.ParseEndpointsConfig(configFile)
	rf.persister = persister.MakePersister(rf.peersEndPoints.DbPath)

	rf.state = FOLLOWER
	rf.voteCount = 0
	rf.chanCommit = make(chan int)
	rf.chanHeartbeat = make(chan int)
	rf.chanGrantVote = make(chan int)
	rf.chanLeader = make(chan bool)
	rf.chanApply = make(chan ApplyMsg)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{LogIndex: 0, LogTerm: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	//hot upgrade
	rf.readPersist()
}

func (rf *Raft) startServer() {
	rpc.Register(rf)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", rf.peersEndPoints.Local.Endpoint)
	config.CheckError("listen error:", err)
	go http.Serve(l, nil)

	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				select {
				case <-rf.chanHeartbeat:
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(time.Duration(FOLLOWERTIMEOUT))):
					rf.state = CANDIDATE
				}
			case LEADER:
				rf.broadcastAppendEntries()
				time.Sleep(HBINTERVAL)
			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.peersEndPoints.Local.ServerId
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()

				/* wins the election
				 * another server establishes itself as leader
				 * or a period of time goes by with no winner
				 */
				go rf.broadcastRequestVote()

				select {
				case <-time.After(time.Duration(rand.Int31()%830+110) * time.Millisecond):
				case <-rf.chanHeartbeat:
					rf.state = FOLLOWER
				case <-rf.chanLeader:
					rf.mu.Lock()

					rf.state = LEADER
					rf.nextIndex = make(map[int]int)
					rf.matchIndex = make(map[int]int)
					for _, peer := range rf.peersEndPoints.Peers {
						rf.nextIndex[peer.ServerId] = rf.getLastIndex() + 1
						rf.matchIndex[peer.ServerId] = 0
					}

					rf.mu.Unlock()
				}
			}
		}
	}()

	go func() {
		for {
			//apply log into stat-machine
			select {
			case <-rf.chanCommit:
				//fmt.Println("Commit", rf.peersEndPoints.Local.ServerId)

				rf.mu.Lock()

				commitIndex := rf.commitIndex
				baseIndex := rf.log[0].LogIndex

				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].LogCommand}
					rf.chanApply <- msg //apply log into stat-machine//
					rf.lastApplied = i
				}

				//when len(log) > threshold_value, should truncate the applied log//
				if rf.lastApplied-baseIndex > LOGTHRESHOLD {
					rf.log = rf.log[rf.lastApplied-LOGTHRESHOLD/2:]
				}

				rf.mu.Unlock()
			}
		}
	}()

	go func() {
		for item := range rf.chanApply {
			if t, ok := item.Command.(LogCmd); ok {
				switch t.Cmd {
				case PUT:
					//fmt.Println("Apply PUT", rf.peersEndPoints.Local.ServerId)
					rf.persister.Put(t.Key, t.Value)
				case DEL:
					rf.persister.Del(t.Key)
				case GET:
				}
			}
		}
	}()

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, os.Kill)

	go func() {
		<-ch
		rf.persist()
		os.Exit(0)
	}()
}

/*
 *following util functions are not thread-safe, invoker must guard it
 */
func (rf *Raft) getState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

func (rf *Raft) persist() { //
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	//rf.mu.Lock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)
	//rf.mu.Unlock()

	value := w.Bytes()
	key := strconv.Itoa(rf.peersEndPoints.Local.ServerId)

	rf.persister.SaveRaftState([]byte(key), value)
}

func (rf *Raft) readPersist() {
	key := strconv.Itoa(rf.peersEndPoints.Local.ServerId)

	value, err := rf.persister.ReadRaftState([]byte(key))
	config.CheckError("readPersist", err)

	r := bytes.NewBuffer(value)
	d := gob.NewDecoder(r)

	//rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
	d.Decode(&rf.nextIndex)
	d.Decode(&rf.matchIndex)
	//rf.mu.Unlock()
}

/*
 * following RPC code
 */
func (rf *Raft) connectToServer(serverId int) (*rpc.Client, error) {
	var serverEndpoint string = ""
	for _, peer := range rf.peersEndPoints.Peers {
		if peer.ServerId == serverId {
			serverEndpoint = peer.Endpoint
			break
		}
	}

	if "" == serverEndpoint {
		return nil, errors.New("No such server " + strconv.Itoa(serverId))
	}

	cli, err := rpc.DialHTTP("tcp", serverEndpoint)
	if err != nil {
		//fmt.Println("Dail", serverEndpoint, err)
		return nil, err
	}

	return cli, nil
}

func (rf *Raft) sendRequestVote(serverId int, args RequestVoteArgs, reply *RequestVoteReply) error {
	cli, err := rf.connectToServer(serverId)
	if err != nil {
		//fmt.Println("sendRequestVote", serverId, err)
		return err
	}

	defer cli.Close()

	err = cli.Call("Raft.RequestVote", args, reply)
	if err != nil {
		//fmt.Println("Call Raft.RequestVote", serverId, err)
		return err
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != CANDIDATE {
		return nil
	}

	term := rf.currentTerm
	if args.Term != term {
		return nil
	}

	if reply.Term > term {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1 //
		rf.persist()     //
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.state == CANDIDATE && rf.voteCount > len(rf.peersEndPoints.Peers)/2 {
			rf.state = LEADER
			rf.chanLeader <- true
		}
	}

	return nil
}

//request vote RPC handler
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() //

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	term := rf.getLastTerm() //if log not exists, term & index will be -1
	index := rf.getLastIndex()

	uptoData := false

	if args.LastLogTerm > term {
		uptoData = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index {
		uptoData = true
	}

	if (-1 == rf.votedFor || rf.votedFor == args.CandidateId) && uptoData {
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

		rf.chanGrantVote <- 1
	}

	return nil
}

func (rf *Raft) sendAppendEntries(serverId int, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cli, err := rf.connectToServer(serverId)
	if err != nil {
		return err
	}

	defer cli.Close()

	err = cli.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		//fmt.Println("sendAppendEntries dail error", err)
		return err
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER || args.Term != rf.currentTerm {
		return nil
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()

		return nil
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[serverId] = args.Entries[len(args.Entries)-1].LogIndex + 1
			rf.matchIndex[serverId] = rf.nextIndex[serverId] - 1
		}
	} else {
		rf.nextIndex[serverId] = reply.NextIndex
	}

	return nil
}

//append entries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.currentTerm {
		//reply.NextIndex = rf.getLastIndex()+1
		reply.Term = rf.currentTerm
		return nil
	}

	rf.chanHeartbeat <- 1
	rf.leaderId = args.LeaderId

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = args.Term

	if args.PreLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		return nil
	}

	baseIndex := rf.log[0].LogIndex
	if args.PreLogIndex > baseIndex {
		term := rf.log[args.PreLogIndex-baseIndex].LogTerm
		if args.PreLogTerm != term {
			for i := args.PreLogIndex - 1; i >= baseIndex; i-- {
				if rf.log[i-baseIndex].LogTerm != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return nil
		}
	}

	if args.PreLogIndex < baseIndex {
		//must keep lastApplied > baseIndex, so program can't be here
	} else {
		rf.log = rf.log[:args.PreLogIndex+1-baseIndex]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}

	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}

		//notify save log to leveldb//
		rf.chanCommit <- 1
	}

	return nil
}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.peersEndPoints.Local.ServerId
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()

	for _, peer := range rf.peersEndPoints.Peers {
		if rf.state == CANDIDATE {
			go func(sid int) {
				var reply RequestVoteReply
				rf.sendRequestVote(sid, args, &reply)
			}(peer.ServerId)
		}
	}
}

func (rf *Raft) broadcastAppendEntries() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/* if there exists an N such that N > commitIndex,
	 * a majority of matchIndex[i] >= N,
	 * and log[N].term == currentTerm: set commitIndex = N
	 */
	N := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex

	for i := N + 1; i <= last; i++ {
		num := 1
		for _, peer := range rf.peersEndPoints.Peers {
			if rf.matchIndex[peer.ServerId] >= i && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
				num++
			}
		}

		if 2*num > (len(rf.peersEndPoints.Peers) + 1) {
			N = i
		}
	}

	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- 1
	}

	//append logs to followers
	for _, peer := range rf.peersEndPoints.Peers {
		if rf.state == LEADER {
			//if have no more log append, so seng empty entry as heartbeat
			if rf.nextIndex[peer.ServerId] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.peersEndPoints.Local.ServerId
				args.LeaderCommit = rf.commitIndex
				args.PreLogIndex = rf.nextIndex[peer.ServerId] - 1
				args.PreLogTerm = rf.log[args.PreLogIndex-baseIndex].LogTerm
				args.Entries = make([]LogEntry, len(rf.log[args.PreLogIndex+1-baseIndex:]))
				copy(args.Entries, rf.log[args.PreLogIndex+1-baseIndex:])

				go func(sid int, parm AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(sid, parm, &reply)
				}(peer.ServerId, args)
			}
		}
	}

	return nil
}

/*
 * provide interface for client
 */
func (rf *Raft) Get(args GetArgs, reply *GetReply) error {
	if !rf.isLeader() {
		//redirect request to leader
		cli, err := rf.connectToServer(rf.leaderId)
		if err != nil {
			return err
		}
		defer cli.Close()

		return cli.Call("Raft.Get", args, reply)
	}

	//is leader, process Get request, fetch data from leveldb
	reply.Ok = false
	buf, err := rf.persister.Get(args.Key)
	if err == nil {
		reply.Value = buf
		reply.Ok = true
	}
	return err
}

func (rf *Raft) Put(args PutArgs, reply *PutReply) error {
	if !rf.isLeader() {
		//redirect request to leader

		//fmt.Println("I am not leader, redirect to leader", rf.peersEndPoints.Local.ServerId)

		cli, err := rf.connectToServer(rf.leaderId)
		if err != nil {
			return err
		}
		defer cli.Close()

		return cli.Call("Raft.Put", args, reply)
	}

	//is leader, process Put request
	cmd := LogCmd{Cmd: PUT, Key: args.Key, Value: args.Value}

	//fmt.Println("I am leader", rf.peersEndPoints.Local.ServerId, cmd.Cmd, string(cmd.Key), string(cmd.Value))

	rf.mu.Lock()

	idx := rf.getLastIndex() + 1
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{LogIndex: idx, LogTerm: term, LogCommand: cmd})

	rf.persist()
	rf.mu.Unlock()

	//wait for PUT success, or timeout
	ch := make(chan int, 1)
	go func() {
		for {
			time.Sleep(HBINTERVAL)
			rf.mu.Lock()
			if rf.commitIndex >= idx {
				ch <- 1
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
	}()

	select {
	case <-time.After(time.Duration(args.TimeoutInterval)):
		reply.Ok = false
	case <-ch:
		reply.Ok = true
	}

	return nil
}

func (rf *Raft) Del(args DelArgs, reply *DelReply) error {
	if !rf.isLeader() {
		//redirect request to leader
		cli, err := rf.connectToServer(rf.leaderId)
		if err != nil {
			return err
		}
		defer cli.Close()

		return cli.Call("Raft.Del", args, reply)
	}

	//is leader, process Del request
	cmd := LogCmd{Cmd: DEL, Key: args.Key}

	rf.mu.Lock()

	idx := rf.getLastIndex() + 1
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{LogIndex: idx, LogTerm: term, LogCommand: cmd})

	rf.persist()
	rf.mu.Unlock()

	//wait for DEL success, or timeout
	ch := make(chan int, 1)
	go func() {
		for {
			time.Sleep(HBINTERVAL)
			rf.mu.Lock()
			if rf.commitIndex >= idx {
				ch <- 1
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
	}()

	select {
	case <-time.After(time.Duration(args.TimeoutInterval)):
		reply.Ok = false
	case <-ch:
		reply.Ok = true
	}

	return nil
}
