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
	"fmt"
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
	state     uint8
	voteCount int
	timer     *time.Timer
	postpone  chan struct{}

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

func slog(s string, args ...interface{}) {
	new_fmt := fmt.Sprintf("%s - %s", time.Now().Format("2006-01-02 15:04:05.000000"), s)
	fmt.Printf(new_fmt, args...)
}

func (rf *Raft) slog(s string, args ...interface{}) {
	state_name := []string{"Follower", "Candidate", "Leader"}
	slog("%d[%d, %s, %d]: %s", rf.me, rf.currentTerm, state_name[rf.state], len(rf.log), fmt.Sprintf(s, args...))
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
		panic("decode error\n")
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
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// rf.slog("receive RV with idx %d term %d from %d[%d]...outdated\n", args.LastLogIndex, args.LastLogTerm, args.CandidateId, args.Term)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		// rf.slog("change to follower because receive RV from %d[%d], term %d -> %d\n", args.CandidateId, args.Term, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.postpone <- struct{}{}
		// rf.slog("receive RV with idx %d term %d from %d[%d]...grant\n", args.LastLogIndex, args.LastLogTerm, args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
		// if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// rf.slog("receive RV with idx %d term %d from %d[%d]...reject, has voted to %d\n", args.LastLogIndex, args.LastLogTerm, args.CandidateId, args.Term, rf.votedFor)
		// } else {
		// rf.slog("receive RV with idx %d term %d from %d[%d]...reject, log too old\n", args.LastLogIndex, args.LastLogTerm, args.CandidateId, args.Term)
		// }
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
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		// rf.slog("receive AE with idx %d term %d log[%d:%d] from %d[%d]...outdated\n", args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries), args.LeaderId, args.Term)
		return
	}

	rf.postpone <- struct{}{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm || rf.state == Candidate {
		// rf.slog("change to follower because receive AE from %d[%d], term %d -> %d\n", args.LeaderId, args.Term, rf.currentTerm, args.Term)
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictTerm = -1
			reply.ConflictIndex = len(rf.log)
			// if len(args.Entries) > 0 {
			// rf.slog("receive AE with idx %d term %d log[%d:%d] from %d[%d]...log missing from index %d\n", args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries), args.LeaderId, args.Term, reply.ConflictIndex)
			// } else {
			// rf.slog("receive AE with idx %d term %d from %d[%d]...log missing from index %d\n", args.PrevLogIndex, args.PrevLogTerm, args.LeaderId, args.Term, reply.ConflictIndex)
			// }
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			for i := 1; i < len(rf.log); i++ {
				if rf.log[i].Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
			// if len(args.Entries) > 0 {
			// rf.slog("receive AE with idx %d term %d log[%d:%d] from %d[%d]...term dismatch, my term %d, matched index %d\n", args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries), args.LeaderId, args.Term, reply.ConflictTerm, reply.ConflictIndex)
			// } else {
			// rf.slog("receive AE with idx %d term %d from %d[%d]...term dismatch, my term %d, matched index %d\n", args.PrevLogIndex, args.PrevLogTerm, args.LeaderId, args.Term, reply.ConflictTerm, reply.ConflictIndex)
			// }
		}
		return
	}

	if len(args.Entries) > 0 {
		var i = 0
		for j := args.PrevLogIndex + 1; i < len(args.Entries) && j < len(rf.log); i, j = i+1, j+1 {
			if rf.log[j].Term != args.Entries[i].Term {
				rf.log = rf.log[:j]
				break
			}
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
		// rf.slog("receive AE with idx %d term %d log[%d:%d] from %d[%d]...ok\n", args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries), args.LeaderId, args.Term)
		// } else {
		// rf.slog("receive AE with idx %d term %d from %d[%d]...heartbeat, ok\n", args.PrevLogIndex, args.PrevLogTerm, args.LeaderId, args.Term)
	}

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	if rf.lastApplied < rf.commitIndex {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			// rf.slog("commit log[%d]=%d\n", i, rf.log[i].Command)
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
				// rf.slog("change to follower because AE reply from %d[%d], term %d -> %d\n", i, reply.Term, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				return
			}
			if reply.Success {
				rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				// rf.slog("update matchIndex[%d] to %d\n", i, rf.matchIndex[i])
				// rf.slog("update nextIndex[%d] to %d\n", i, rf.nextIndex[i])
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
						// rf.slog("commit log[%d]=%d\n", i, rf.log[i].Command)
						rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
					}
					rf.lastApplied = rf.commitIndex
				}
				// rf.slog("update commitIndex to %d\n", rf.commitIndex)
			} else {
				if reply.ConflictTerm == -1 {
					rf.nextIndex[i] = reply.ConflictIndex
					// rf.slog("update nextIndex[%d] to ConflictIndex %d\n", i, rf.nextIndex[i])
				} else {
					var j int
					for j = args.PrevLogIndex; j >= 0; j-- {
						if rf.log[j].Term == reply.ConflictTerm {
							rf.nextIndex[i] = j + 1
							// rf.slog("update nextIndex[%d] to ConflictIndex %d\n", i, rf.nextIndex[i])
							break
						}
					}
					if j == -1 {
						rf.nextIndex[i] = reply.ConflictIndex
						// rf.slog("update nextIndex[%d] to ConflictIndex %d\n", i, rf.nextIndex[i])
					}
				}
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
		// rf.slog("Start failed\n", rf.me, rf.currentTerm)
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	// rf.slog("Start %d, log length %d\n", command, len(rf.log))
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
	// rf.slog("start election for %d\n", rf.currentTerm+1)
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
			// rf.slog("send vote request to %d\n", server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					rf.voteCount++
					if rf.state == Candidate && rf.voteCount > len(rf.peers)/2 {
						rf.state = Leader
						// rf.slog("become leader\n")
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
						}
						go func() {
							for !rf.killed() && rf.state == Leader {
								rf.sendAppendEntriesAll()
								time.Sleep(100 * time.Millisecond)
							}
						}()
					}
				}
				if reply.Term > rf.currentTerm {
					// rf.slog("change to follower because receive RV reply from %d[%d], term %d -> %d\n", server, args.Term, rf.currentTerm, args.Term)
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
	rf.timer = time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	rf.postpone = make(chan struct{})
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.postpone:
			if !rf.timer.Stop() {
				<-rf.timer.C
			}
			rf.timer.Reset(time.Duration(rand.Intn(150)+150) * time.Millisecond)

		case <-rf.timer.C:
			rf.timer.Reset(time.Duration(rand.Intn(150)+150) * time.Millisecond)
			if rf.state == Follower || rf.state == Candidate {
				rf.startElection()
			}
		}
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
