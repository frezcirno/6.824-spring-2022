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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
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

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     int
	voteCount int
	timer     *time.Timer

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// for leader
	nextIndex  []int
	matchIndex []int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// fmt.Printf("decode error\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("%d[%d]: request vote from %d, [%d]=%d, my %d log\n", rf.me, rf.currentTerm, args.CandidateId, args.LastLogIndex, args.LastLogTerm, len(rf.log))
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		// fmt.Printf("%d[%d]: vote to %d\n", rf.me, rf.currentTerm, args.CandidateId)
	} else {
		reply.VoteGranted = false
		// fmt.Printf("%d[%d]: reject to %d, my %d log [%d]!=x, or already vote to %d\n", rf.me, rf.currentTerm, args.CandidateId, len(rf.log), args.LastLogIndex, rf.votedFor)
	}
	rf.persist()
	reply.Term = rf.currentTerm
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
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
	if !rf.timer.Stop() {
		<-rf.timer.C
	}
	rf.timer.Reset(time.Duration(rand.Intn(300)+300) * time.Millisecond)
	// fmt.Printf("%d[%d]: receive AppendEntries with log[%d]=%d, %d logs from %d[%d]\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		// fmt.Printf("%d[%d]: change to follower, term %d -> %d\n", rf.me, rf.currentTerm, rf.currentTerm, args.Term)
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if rf.state != Follower {
		// fmt.Printf("%d[%d]: change to follower\n", rf.me, rf.currentTerm)
		rf.state = Follower
	}
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		// fmt.Printf("%d[%d]: AppendEntries failed, log not match\n", rf.me, rf.currentTerm)
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persist()
	// fmt.Printf("%d[%d]: AppendEntries success, log length %d\n", rf.me, rf.currentTerm, len(rf.log))
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	if rf.lastApplied < rf.commitIndex {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			// fmt.Printf("%d[%d]: commit log[%d]=%d\n", rf.me, rf.currentTerm, i, rf.log[i].Command)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
		}
		rf.lastApplied = rf.commitIndex
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesAll() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			nextIndex := rf.nextIndex[i]
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.log[nextIndex-1].Term,
				Entries:      rf.log[nextIndex:],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rv := rf.sendAppendEntries(i, args, reply)
			if !rv {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				// fmt.Printf("%d[%d]: change to follower, term %d -> %d\n", rf.me, rf.currentTerm, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				return
			}
			if reply.Success {
				rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				// fmt.Printf("%d[%d]: update matchIndex[%d] to %d\n", rf.me, rf.currentTerm, i, rf.matchIndex[i])
				// fmt.Printf("%d[%d]: update nextIndex[%d] to %d\n", rf.me, rf.currentTerm, i, rf.nextIndex[i])
				for j := rf.commitIndex + 1; j < len(rf.log); j++ {
					count := 1
					for k := range rf.peers {
						if k == rf.me {
							continue
						}
						if rf.matchIndex[k] >= j {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = j
					}
				}
				if rf.commitIndex > rf.lastApplied {
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
						// fmt.Printf("%d[%d]: commit log[%d]=%d\n", rf.me, rf.currentTerm, i, rf.log[i].Command)
						rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
					}
					rf.lastApplied = rf.commitIndex
				}
				// fmt.Printf("%d[%d]: update commitIndex to %d\n", rf.me, rf.currentTerm, rf.commitIndex)
			} else {
				rf.nextIndex[i]--
			}
		}(i)
	}
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		// fmt.Printf("%d[%d]: Start failed\n", rf.me, rf.currentTerm)
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	// fmt.Printf("%d[%d]: Submit %d, log length %d\n", rf.me, rf.currentTerm, command, len(rf.log))
	rf.persist()
	rf.sendAppendEntriesAll()
	return len(rf.log) - 1, rf.currentTerm, true
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

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()
	rf.mu.Unlock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			// fmt.Printf("%d[%d]: send vote request to %d\n", rf.me, rf.currentTerm, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					// fmt.Printf("%d[%d]: get vote from %d\n", rf.me, rf.currentTerm, server)
					rf.voteCount++
					if rf.state == Candidate && rf.voteCount > len(rf.peers)/2 {
						// fmt.Printf("%d[%d]: become leader\n", rf.me, rf.currentTerm)
						rf.state = Leader
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
						}
						go func() {
							for !rf.killed() && rf.state == Leader {
								rf.sendAppendEntriesAll()
								time.Sleep(150 * time.Millisecond)
							}
						}()
					}
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Follower
					rf.persist()
				}
			}
		}(i)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.timer.Reset(time.Duration(rand.Intn(300)+300) * time.Millisecond)
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<-rf.timer.C
		if rf.state == Follower || rf.state == Candidate {
			// fmt.Printf("%d[%d]: start election\n", rf.me, rf.currentTerm)
			rf.startElection()
		}
		rf.timer.Reset(time.Duration(rand.Intn(300)+300) * time.Millisecond)
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.voteCount = 0
	rf.timer = time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // index 0 is dummy
	rf.log[0] = LogEntry{Term: 0, Command: nil}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
