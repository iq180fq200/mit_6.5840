package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverState string

const (
	Leader    serverState = "L"
	Candidate serverState = "C"
	Follower  serverState = "F"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry
	//for time out checking
	resetTime bool

	//volatile states on all servers
	commitIndex int
	lastApplied int
	state       serverState
	commit      chan bool

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//volatile state on candidate
	vote_num int

	//no need to use a lock variables(read only once created or only used by one process)
	peerNum int
}

type LogEntry struct {
	Index   int
	Term    int
	Content interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	//Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var _logs []LogEntry
	var _currentTerm int
	var _votedFor int
	if d.Decode(&_logs) != nil ||
		d.Decode(&_currentTerm) != nil || d.Decode(&_votedFor) != nil {
		Debug(dError, "S%d cannot unmarshal the persisted data", rf.me)
	} else {
		rf.logs = _logs
		rf.currentTerm = _currentTerm
		rf.votedFor = _votedFor
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//optimize
	XTerm  int //term in the conflicting entry (if any)
	XIndex int //index of first entry with that term (if any)
	XLen   int //log length
}
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendAndDealAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, neglectFailure bool) {
	ok := false
	if args.Entries == nil {
		Debug(dHeart, "S%d send heartbeat to S%d", rf.me, server)
	} else {
		Debug(dLeader, "S%d send %d log entries to S%d, last entry I%d", rf.me, len(args.Entries), server, args.Entries[len(args.Entries)-1].Index)
	}
	for !ok && !rf.killed() {
		ok = rf.sendAppendEntries(server, args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if it's not a leader anymore
	if args.Term != rf.currentTerm || rf.state != Leader {
		return
	}
	if rf.currentTerm < reply.Term {
		a := rf.currentTerm
		rf.resetTime = true
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		Debug(dPersist, "S%d persist due to term change", rf.me)
		Debug(dTerm, "S%d is outdated, leader -> follower, T%d->T%d", rf.me, a, reply.Term)
		return
	}

	if !reply.Success {
		if neglectFailure {
			return
		}
		rf.nextIndex[server]--
		if reply.XTerm != -1 {
			last := rf.hasAndLast(reply.XTerm)
			Debug(dLog, "S%d get hint from S%d and find last is %d, current nextIndex is %d", rf.me, server, last, rf.nextIndex[server]+1)
			if last != -1 && last < rf.nextIndex[server] {
				rf.nextIndex[server] = last
			} else if last == -1 && reply.XIndex != -1 && reply.XIndex < rf.nextIndex[server] {
				rf.nextIndex[server] = reply.XIndex
			}
		} else if reply.XLen < rf.nextIndex[server] {
			rf.nextIndex[server] = max(reply.XLen, 1)
		}
		Debug(dLog, "S%d nextIndex[%d] decrease to I%d", rf.me, server, rf.nextIndex[server])
	} else if args.Entries == nil {
		rf.matchIndex[server] = max(args.PrevLogIndex, rf.matchIndex[server])
		rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex+1)
	} else {
		rf.matchIndex[server] = max(args.Entries[len(args.Entries)-1].Index, rf.matchIndex[server])
		rf.nextIndex[server] = max(rf.nextIndex[server], args.Entries[len(args.Entries)-1].Index+1)
		Debug(dLeader, "S%d appendEntry consent to S%d, nextIndex is %d", rf.me, server, rf.nextIndex[server])
		//check to see if can be committed
		rf.tryCommit(args.Entries)
	}
}

func max(i int, i2 int) int {
	if i < i2 {
		return i2
	} else {
		return i
	}

}

func (rf *Raft) sendAndDealRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	Debug(dServer, "s%d send request vote to s%d", rf.me, server)
	ok := false
	for !ok && !rf.killed() {
		ok = rf.sendRequestVote(server, args, reply)
	}
	rf.mu.Lock()
	if args.Term != rf.currentTerm || rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm < reply.Term {
		rf.resetTime = true
		Debug(dTimer, "S%d reset timer", rf.me)
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		Debug(dPersist, "S%d persist due to term change", rf.me)
		Debug(dTerm, "S%d term -> T%d, State candidate -> follower", rf.me, reply.Term)
		rf.mu.Unlock()
		return
	}
	if reply.VoteGranted {
		Debug(dVote, "S%d get vote <- S%d", rf.me, server)
		rf.vote_num++
		//if already win
		if rf.vote_num > rf.peerNum*1.0/2 {
			Debug(dLeader, "S%d Achieved Majority for T%d, State candidate -> leader", rf.me, rf.currentTerm)
			rf.initialLeader()
			rf.leaderFunction()
		} else {
			rf.mu.Unlock()
		}
	} else {
		rf.mu.Unlock()
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//deal with the term
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		Debug(dServer, "S%d reject append entry/heartbeat request from S%d, args.term is %d, new term is %d", rf.me, args.LeaderID, args.Term, rf.currentTerm)
		return
	} else if rf.currentTerm < args.Term {
		a := rf.currentTerm
		b := rf.state
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		Debug(dTerm, "S%d get append entry from new term leader S%d, %v -> follower, T%d->T%d", rf.me, args.LeaderID, b, a, rf.currentTerm)
		rf.persist()
		Debug(dPersist, "S%d persist state due to go into new term", rf.me)
	} else {
		if rf.state == Candidate {
			rf.state = Follower
			Debug(dTerm, "S%d find other leader S%d win, candidate -> follower", rf.me, args.LeaderID)
		}
	}

	//after consent on term and roles, do the following

	if rf.state == Leader {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	// ！only followers will do the following steps
	rf.resetTime = true
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1
	//deal with the entries******************************
	//1. output logs
	if args.Entries == nil {
		//heartbeat
		Debug(dTimer, "S%d get heartbeat from leader (S%d,T%d), reset timer", rf.me, args.LeaderID, args.Term)
	} else {
		//commands
		Debug(dTimer, "S%d get entries from leader (S%d,T%d), reset timer", rf.me, args.LeaderID, args.Term)
	}
	//2. try to find the log that match with prev-log and to delete all the unmatched logs
	if args.PrevLogIndex != 0 {
		//scan the logs
		if len(rf.logs) == 0 {
			//must be failed
			reply.Success = false
			reply.XLen = 0
		} else {
			ok := rf.scanCleanFindprevAppendLog(args, reply)
			if !ok {
				reply.Success = false
			} else {
				reply.Success = true
			}
		}
	} else {
		rf.logs = args.Entries
		rf.persist()
		Debug(dPersist, "S%d persist due to new logs appended", rf.me)
		Debug(dLog, "S%d append entries (T%d,I%d) from leader (S%d,T%d) succeed", rf.me, rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index, args.LeaderID, args.Term)
		reply.Success = true
	}
	//3. deal with the commit and apply
	if reply.Success && len(rf.logs) > 0 && args.LeaderCommit > rf.commitIndex {
		if rf.logs[len(rf.logs)-1].Index < args.LeaderCommit-1 {
			//if logs hasn't reached leader's commit point
			rf.commitIndex = rf.logs[len(rf.logs)-1].Index + 1
			Debug(dCommit, "S%d commitIndex %d", rf.me, rf.commitIndex)
			rf.wakeupCommiter()

		} else {
			rf.commitIndex = args.LeaderCommit
			Debug(dCommit, "S%d commitIndex %d", rf.me, rf.commitIndex)
			rf.wakeupCommiter()
		}
	}
	return
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//deal with the terms
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d reject vote request from S%d, new term is %d", rf.me, args.CandidateID, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		a := rf.currentTerm
		b := rf.state
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		Debug(dPersist, "S%d persist due to term change")
		Debug(dTerm, "S%d get voteRequest from newer term S%d, T%d->T%d, %v->Follower", rf.me, args.CandidateID, a, args.Term, b)
	}
	//deal with the vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		//Debug(dVote, "S%d gotten new vote: T%d, ID%d", rf.me, args.LastLogTerm, args.LastLogIndex)
		if rf.logs == nil || rf.getLastLog().Term < args.LastLogTerm {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.persist()
			Debug(dPersist, "S%d persist due to vote")
			Debug(dVote, "S%d vote for S%d", rf.me, rf.votedFor)
			rf.resetTime = true
			Debug(dTimer, "S%d make valid vote, reset timer", rf.me)
		} else if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && rf.logs[len(rf.logs)-1].Index <= args.LastLogIndex {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.persist()
			Debug(dPersist, "S%d persist due to vote")
			Debug(dVote, "S%d vote for S%d", rf.me, rf.votedFor)
			rf.resetTime = true
			Debug(dTimer, "S%d make valid vote, reset timer", rf.me)
		} else {
			reply.VoteGranted = false
			Debug(dVote, "S%d reject vote to S%d, has a newer log", rf.me, args.CandidateID)
		}
	} else {
		Debug(dVote, "S%d reject vote->%d, already voted for %d", rf.me, args.CandidateID, rf.votedFor)
	}
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isLeader = true
		term = rf.currentTerm
		if rf.logs != nil && len(rf.logs) != 0 {
			index = rf.getLastLog().Index + 1
		} else {
			index = 1
		}
		newEntry := LogEntry{
			Index:   index,
			Term:    rf.currentTerm,
			Content: command,
		}
		rf.logs = append(rf.logs, newEntry)
		rf.matchIndex[rf.me] = newEntry.Index
		rf.nextIndex[rf.me] = newEntry.Index + 1
		Debug(dLog, "S%d append entries (T%d,I%d) as leader succeed", rf.me, newEntry.Term, newEntry.Index)
		var t []LogEntry
		t = append(t, newEntry)
		rf.tryCommit(t)
		//try to send the log to the followers, if return false, then the heartbeat would
		//help to send after all the logs are followed up
		for i := 0; i < rf.peerNum; i++ {
			if i == rf.me {
				continue
			}
			appendEntriesArgs := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				Entries:      []LogEntry{newEntry},
				PrevLogIndex: index - 1,
				LeaderCommit: rf.commitIndex,
			}
			if index != 1 {
				appendEntriesArgs.PrevLogTerm = rf.getLog(index - 1).Term
			}
			appendEntriesReply := AppendEntriesReply{}
			go rf.sendAndDealAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, true)
		}
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		//if time out
		if rf.resetTime == false && rf.state != Leader {
			rf.currentTerm++
			t := rf.state
			rf.state = Candidate
			Debug(dTimer, "S%d time out as a %v, rise term to %d, become a new candidate", rf.me, t, rf.currentTerm)
			rf.votedFor = rf.me
			Debug(dVote, "S%d vote for S%d", rf.me, rf.me)
			rf.persist()
			Debug(dPersist, "S%d persist its term and voted for as a new candidate", rf.me)
			rf.vote_num = 1
			Debug(dVote, "S%d get vote <- S%d", rf.me, rf.me)
			requestVoteArgs := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateID: rf.me,
			}
			if rf.logs == nil {
				requestVoteArgs.LastLogIndex = 0
				requestVoteArgs.LastLogTerm = -1
			} else {
				requestVoteArgs.LastLogIndex = rf.getLastLog().Index
				requestVoteArgs.LastLogTerm = rf.getLastLog().Term
			}
			for i := 0; i < rf.peerNum; i++ {
				if i == rf.me {
					continue
				}
				requestVoteReply := RequestVoteReply{
					Term:        0,
					VoteGranted: false,
				}
				go rf.sendAndDealRequestVote(i, &requestVoteArgs, &requestVoteReply)
			}
		}
		//begin another timeout
		rf.resetTime = false
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 250 + (rand.Int63() % 300)
		//ms := 50 + (rand.Int63() % 30)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) commiter(ch chan ApplyMsg) {
	for rf.killed() == false {
		select {
		case <-rf.commit:
			rf.mu.Lock()
			Debug(dCommit, "S%d commiter get signal", rf.me)
			la := rf.lastApplied
			ci := rf.commitIndex
			me := rf.me
			rf.mu.Unlock()
			for i := la + 1; i < ci; i++ {
				rf.mu.Lock()
				msg := ApplyMsg{
					CommandValid:  true,
					Command:       rf.getLog(i).Content,
					CommandIndex:  rf.getLog(i).Index,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				rf.mu.Unlock()
				ch <- msg
				rf.mu.Lock()
				rf.lastApplied = rf.getLog(i).Index
				rf.mu.Unlock()
				Debug(dCommit, "S%d applied log %d", me, msg.CommandIndex)
			}
		default:

		}
	}
}

func (rf *Raft) leaderFunction() {
	isLeader := true
	//become leader and make heartbeats from time to time
	for (!rf.killed()) && (isLeader) {
		rf.mu.Lock()
		for i := 0; i < rf.peerNum; i++ {
			if i == rf.me {
				continue
			}
			appendEntriesArgs := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				Entries:      nil,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}
			//if has prevlog, then has prevlogTerm
			if rf.nextIndex[i] != 1 {
				appendEntriesArgs.PrevLogTerm = rf.getLog(rf.nextIndex[i] - 1).Term
			}
			//if not the newest, send all the entries after the consent one
			if rf.logs != nil && appendEntriesArgs.PrevLogIndex != rf.getLastLog().Index {
				appendEntriesArgs.Entries = rf.getLogSlice(appendEntriesArgs.PrevLogIndex+1, -1)
			}
			if i != rf.me {
				appendEntriesReply := AppendEntriesReply{}
				go rf.sendAndDealAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, false)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(105) * time.Millisecond)
		_, isLeader = rf.GetState()
	}
}

func (rf *Raft) initialLeader() {
	rf.state = Leader
	rf.matchIndex = make([]int, rf.peerNum)
	rf.nextIndex = make([]int, rf.peerNum)
	for i := 0; i < rf.peerNum; i++ {
		rf.matchIndex[i] = 0
		if rf.logs != nil {
			rf.nextIndex[i] = rf.getLastLog().Index + 1
		} else {
			rf.nextIndex[i] = 1
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) wakeupCommiter() {
	if rf.commitIndex > rf.lastApplied+1 {
		select {
		case rf.commit <- true:
			Debug(dCommit, "S%d send commit signal to commiter", rf.me)
		default:
		}
	}
}

func (rf *Raft) getLog(index int) *LogEntry {
	return &rf.logs[index-1]
}
func (rf *Raft) getLastLog() *LogEntry {
	return &rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getLogSlice(start int, end int) []LogEntry {
	if start == -1 {
		return rf.logs[:end-1]
	}
	if end == -1 {
		return rf.logs[start-1:]
	}
	return rf.logs[start-1 : end-1]
}

func (rf *Raft) scanCleanFindprevAppendLog(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//not append directly, find where it matches,if no return false; else return true
	ok := false
	var minBound int
	var maxBound int
	if len(args.Entries) != 0 {
		minBound = args.Entries[0].Index
		maxBound = args.Entries[len(args.Entries)-1].Index
	} else {
		minBound = -1
		maxBound = -1
	}
	for i := len(rf.logs) - 1; i >= 0; i-- {
		//check if conflict and delete conflict
		if rf.logs[i].Index <= maxBound && rf.logs[i].Index >= minBound {
			if rf.logs[i].Term != args.Entries[rf.logs[i].Index-minBound].Term {
				Debug(dLog, "S%d has conflicted log (T%d,I%d) from leader (T%d,I%d)", rf.me, rf.logs[i].Term, rf.logs[i].Index, args.Entries[0+rf.logs[i].Index-minBound].Term, args.Entries[0+rf.logs[i].Index-minBound].Index)
				//delete all that follows(including i)
				reply.XTerm = rf.logs[i].Term
				rf.logs = rf.logs[:i]
				continue
			}
		}
		//check the first matched entry and append
		if args.PrevLogIndex == rf.logs[i].Index && args.PrevLogTerm == rf.logs[i].Term {
			ok = true
			if len(rf.logs[i+1:]) < len(args.Entries) {
				rf.logs = rf.logs[:i+1]
				rf.logs = append(rf.logs, args.Entries...)
				rf.persist()
				Debug(dLog, "S%d persist due to append entries", rf.me)
				Debug(dLog, "S%d append entries (T%d,I%d) from leader (S%d,T%d) succeed", rf.me, rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index, args.LeaderID, args.Term)
			} else {
				//Debug(dLog, "S%d has newer entries than the current qppend entries request from leader (S%d,T%d),leader entries: %v, I have %v", rf.me, args.LeaderID, args.Term, args.Entries, rf.logs[i+1:])
				//Debug(dLog, "S%d has newer entries than the current append entries request from leader (S%d,T%d)", rf.me, args.LeaderID, args.Term)
			}
			break
		}
	}
	if !ok {
		if reply.XTerm != -1 {
			reply.XIndex = rf.findFirstIndex(reply.XTerm)
			Debug(dLog, "S%d send back hint to leader (XT%d,firstXI%d)", rf.me, reply.XTerm, reply.XIndex)
		} else {
			reply.XLen = rf.getLastLog().Index
		}
		Debug(dLog, "S%d no matched prev-entry(T%d,I%d)", rf.me, args.PrevLogTerm, args.PrevLogIndex)
	}
	return ok
}

func (rf *Raft) tryCommit(entries []LogEntry) {
	if entries[len(entries)-1].Term == rf.currentTerm {
		count := 0
		candidateInd := entries[len(entries)-1].Index
		for i := 0; i < rf.peerNum; i++ {
			if rf.matchIndex[i] >= candidateInd {
				count++
			}
		}
		if count > rf.peerNum*1.0/2 {
			rf.commitIndex = candidateInd + 1
			rf.wakeupCommiter()
			Debug(dCommit, "S%d commitIndex to %d", rf.me, rf.commitIndex)
		}
	}
}

func (rf *Raft) findFirstIndex(term int) int {
	result := -1
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Term < term {
			return result
		}
		if rf.logs[i].Term == term {
			result = rf.logs[i].Index
		}
	}
	return result
}

func (rf *Raft) hasAndLast(term int) int {
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Term == term {
			return rf.logs[i].Index
		}
	}
	return -1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		//currentTerm:  ,
		logs:        nil,
		commitIndex: 1,
		lastApplied: 0,
		state:       Follower,
		nextIndex:   nil,
		matchIndex:  nil,
		resetTime:   true,
		votedFor:    -1,
		commit:      make(chan bool, 1),
		currentTerm: 0,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.peerNum = len(rf.peers)
	Debug(dServer, "s%d started, term is t%d, voted for S%d, last log index I%d", rf.me, rf.currentTerm, rf.votedFor, rf.getLastLog().Index)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commiter(applyCh)

	return rf
}
