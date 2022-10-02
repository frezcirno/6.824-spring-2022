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
	Follower uint8 = iota
	Candidate
	Leader

	HeartbeatIntervalMs  = 100
	ElectionMinTimeoutMs = 300
	ElectionMaxTimeoutMs = 500

	ElectionRandomTimeoutMs = ElectionMaxTimeoutMs - ElectionMinTimeoutMs
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state          uint8
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	commitCond     *sync.Cond
	replicaCond    []*sync.Cond

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

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
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
	slog("%d[%d, %s, %d-%d-%d-%d]: %s", rf.me, rf.currentTerm, state_name[rf.state], rf.firstLogIndex(), rf.lastApplied, rf.commitIndex, rf.nextLogIndex(), fmt.Sprintf(s, args...))
}

func slog_slice(from int, to int) string {
	if from == to {
		return ""
	}
	if from == to-1 {
		return fmt.Sprintf(" log[%d]", from)
	}
	if from == to-2 {
		return fmt.Sprintf(" log[%d,%d]", from, to-1)
	}
	return fmt.Sprintf(" log[%d-%d]", from, to-1)
}

func (rf *Raft) prevLogIndex() int {
	return rf.log[0].Command.(int)
}

func (rf *Raft) prevLogTerm() int {
	return rf.log[0].Term
}

func (rf *Raft) firstLogIndex() int {
	return rf.prevLogIndex() + 1
}

func (rf *Raft) lastLogIndex() int {
	return rf.prevLogIndex() + len(rf.log) - 1
}

func (rf *Raft) nextLogIndex() int {
	return rf.prevLogIndex() + len(rf.log)
}

func (rf *Raft) getLog(index int) *LogEntry {
	// if index < rf.PrevLogIndex() || index > rf.LastLogIndex() {
	// rf.slog("GetLog %d...panic\n", index)
	// 	panic("")
	// }
	return &rf.log[index-rf.prevLogIndex()]
}

func (rf *Raft) getLogFrom(from int) []LogEntry {
	// if from < rf.FirstLogIndex() || from > rf.NextLogIndex() {
	// rf.slog("GetLogFrom %d...panic", from)
	// 	panic("")
	// }
	return rf.log[from-rf.prevLogIndex():]
}

func (rf *Raft) copyLogFrom(from int) []LogEntry {
	// if from < rf.FirstLogIndex() || from > rf.NextLogIndex() {
	// rf.slog("CopyLogFrom %d...panic", from)
	// 	panic("")
	// }
	res := make([]LogEntry, len(rf.log[from-rf.prevLogIndex():]))
	copy(res, rf.log[from-rf.prevLogIndex():])
	return res
}

func (rf *Raft) getLogSlice(from int, to int) []LogEntry {
	// if from < rf.FirstLogIndex() || to > rf.NextLogIndex() {
	// rf.slog("GetLogSlice from %d to %d...panic", from, to)
	// 	panic("")
	// }
	return rf.log[from-rf.prevLogIndex() : to-rf.prevLogIndex()]
}

func (rf *Raft) getLogIndex(index int) int {
	// local log index -> global log index
	// if index < 1 || index > rf.LogCount() {
	// rf.slog("GetLogIndex %d", index)
	// 	panic("")
	// }
	return rf.prevLogIndex() + index
}

func (rf *Raft) getLogTerm(index int) int {
	return rf.getLog(index).Term
}

func (rf *Raft) switchTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) switchState(state uint8) {
	if rf.state == state {
		return
	}
	// rf.slog("switch state to %d\n", state)
	rf.state = state
	switch state {
	case Follower:
		rf.electionTimer.Reset(makeTimeout())
		rf.heartbeatTimer.Stop()
	case Candidate:
	case Leader:
		nextLogIndex := rf.nextLogIndex()
		for i := range rf.peers {
			rf.nextIndex[i] = nextLogIndex
			rf.matchIndex[i] = 0
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HeartbeatIntervalMs * time.Millisecond)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.persister.SaveRaftState(rf.serialize())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.serialize(), snapshot)
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
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastApplied = rf.prevLogIndex()
	rf.commitIndex = rf.prevLogIndex()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// rf.slog("receive IS idx %d term %d from %d[%d]...outdated\n", args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		// rf.slog("change to follower because receive IS idx %d term %d from %d[%d], term %d -> %d\n", args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId, args.Term, rf.currentTerm, args.Term)
		rf.switchTerm(args.Term)
		rf.persist()
	}

	rf.switchState(Follower)
	rf.electionTimer.Reset(makeTimeout())

	if args.LastIncludedIndex <= rf.commitIndex {
		// rf.slog("receive IS idx %d term %d from %d[%d]...already commit\n", args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId, args.Term)
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		// rf.slog("commit snapshot idx %d term %d from %d[%d]\n", args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId, args.Term)
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		// rf.slog("CondInstallSnapshot idx %d term %d...failed, there are newer commits\n", lastIncludedIndex, lastIncludedTerm)
		return false
	}

	if lastIncludedIndex > rf.lastLogIndex() {
		rf.log = []LogEntry{{Term: lastIncludedTerm, Command: lastIncludedIndex}}
	} else {
		rf.log = rf.getLogFrom(lastIncludedIndex)
		rf.log[0].Command = lastIncludedIndex
	}

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	rf.persistWithSnapshot(snapshot)
	// rf.slog("CondInstallSnapshot idx %d term %d...success\n", lastIncludedIndex, lastIncludedTerm)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.prevLogIndex() {
		// rf.slog("Snapshot at index %d...too small", index)
		return
	}

	rf.log = rf.getLogFrom(index)
	rf.log[0].Command = index

	rf.persistWithSnapshot(snapshot)
	// rf.slog("Snapshot at index %d...ok\n", index)
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

func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	lastLogTerm := rf.log[len(rf.log)-1].Term
	if args.LastLogTerm > lastLogTerm {
		return true
	}
	if args.LastLogTerm == lastLogTerm {
		return args.LastLogIndex >= rf.lastLogIndex()
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// rf.slog("receive RV with idx %d term %d from %d[%d]...outdated\n", args.LastLogIndex, args.LastLogTerm, args.CandidateId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		// rf.slog("change to follower because receive RV from %d[%d], term %d -> %d\n", args.CandidateId, args.Term, rf.currentTerm, args.Term)
		rf.switchTerm(args.Term)
		rf.switchState(Follower)
	}

	if (rf.votedFor != -1 && rf.votedFor != args.CandidateId) || !rf.isUpToDate(args) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// rf.slog("receive RV with idx %d term %d from %d[%d]...reject, has voted to %d\n", args.LastLogIndex, args.LastLogTerm, args.CandidateId, args.Term, rf.votedFor)
		// } else {
		// rf.slog("receive RV with idx %d term %d from %d[%d]...reject, log too old\n", args.LastLogIndex, args.LastLogTerm, args.CandidateId, args.Term)
		// }
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	// rf.slog("receive RV with idx %d term %d from %d[%d]...grant\n", args.LastLogIndex, args.LastLogTerm, args.CandidateId, args.Term)

	reply.VoteGranted = true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		// rf.slog("receive AE with idx %d term %d%s from %d[%d]...outdated\n", args.PrevLogIndex, args.PrevLogTerm, slog_slice(args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries)), args.LeaderId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		// rf.slog("change to follower because receive AE from %d[%d], term %d -> %d\n", args.LeaderId, args.Term, rf.currentTerm, args.Term)
		rf.switchTerm(args.Term)
	}

	rf.switchState(Follower)
	rf.electionTimer.Reset(makeTimeout())
	defer rf.persist()

	if args.PrevLogIndex < rf.prevLogIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.nextLogIndex()
		// rf.slog("receive AE with idx %d term %d%s from %d[%d]...reject, log has been archived\n", args.PrevLogIndex, args.PrevLogTerm, slog_slice(args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries)), args.LeaderId, args.Term)
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() || rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		if args.PrevLogIndex > rf.lastLogIndex() {
			// We don't have the log entry, so we need to find the first log
			// entry that has the same term as the log entry at PrevLogIndex.
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.nextLogIndex()
			// rf.slog("receive AE with idx %d term %d%s from %d[%d]...log missing from index %d\n", args.PrevLogIndex, args.PrevLogTerm, slog_slice(args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries)), args.LeaderId, args.Term, reply.ConflictIndex)
		} else {
			// We have the log entry, but the term doesn't match.
			reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
			// Find the first log entry that has the same term as the log entry
			reply.ConflictIndex = rf.prevLogIndex()
			for idx := 1; idx < len(rf.log); idx++ {
				if rf.log[idx].Term == reply.ConflictTerm {
					reply.ConflictIndex = rf.getLogIndex(idx)
					break
				}
			}
			// rf.slog("receive AE with idx %d term %d%s from %d[%d]...term dismatch, my term %d, matched index %d\n", args.PrevLogIndex, args.PrevLogTerm, slog_slice(args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries)), args.LeaderId, args.Term, reply.ConflictTerm, reply.ConflictIndex)
		}
		return
	}

	if len(args.Entries) > 0 {
		// args.Entries[0] <=> rf.log[args.PrevLogIndex+1]
		var i = 0
		for j := args.PrevLogIndex + 1 - rf.prevLogIndex(); i < len(args.Entries) && j < len(rf.log); i, j = i+1, j+1 {
			if rf.log[j].Term != args.Entries[i].Term {
				rf.log = rf.log[:j]
				break
			}
		}
		rf.log = append(rf.log, args.Entries[i:]...)
	}

	// result := map[bool]string{false: "heartbeat, ok", true: "ok"}[len(args.Entries) > 0]
	if rf.commitIndex < args.LeaderCommit {
		// oldCommitIndex := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, rf.lastLogIndex())
		// result += fmt.Sprintf(", commit %d->%d", oldCommitIndex, rf.commitIndex)
		rf.commitCond.Signal()
	}

	// rf.slog("receive AE with cmt %d term %d%s from %d[%d]...%s\n", args.LeaderCommit, args.PrevLogTerm, slog_slice(args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries)), args.LeaderId, args.Term, result)

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) replicaByIS(i int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	// rf.slog("%d needed log has been archived, send IS...\n", i)
	if rf.sendInstallSnapshot(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm == args.Term && rf.state == Leader {
			if reply.Term > rf.currentTerm {
				// rf.slog("change to follower because IS reply from %d[%d], term %d -> %d\n", i, reply.Term, rf.currentTerm, reply.Term)
				rf.switchTerm(reply.Term)
				rf.switchState(Follower)
				rf.persist()
				return
			} else {
				rf.matchIndex[i] = args.LastIncludedIndex
				rf.nextIndex[i] = args.LastIncludedIndex + 1
			}
		}
	}
}

func heapDown(h []int, i int) {
	for {
		l := 2*i + 1
		if l >= len(h) {
			break
		}
		// find the smallest child
		c := l
		if r := l + 1; r < len(h) && h[r] < h[l] {
			c = r
		}
		// swap with the smallest child if necessary
		if h[i] <= h[c] {
			break
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
}

func heapify(h []int) {
	for i := len(h)/2 - 1; i >= 0; i-- {
		heapDown(h, i)
	}
}

// find the k-th largest element in the array
func topK(nums []int, k int) int {
	h := make([]int, k)
	copy(h, nums[:k])
	heapify(h)
	for i := k; i < len(nums); i++ {
		if nums[i] > h[0] {
			h[0] = nums[i]
			heapDown(h, 0)
		}
	}
	return h[0]
}

func (rf *Raft) updateCommitIndex() {
	commitIndex := topK(rf.matchIndex, (len(rf.peers)-1)/2)
	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		// rf.slog("update commitIndex to %d\n", rf.commitIndex)
		rf.commitCond.Signal()
	}
}

func (rf *Raft) replicaByAE(i int, args *AppendEntriesArgs, nextIndex int) {
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm == args.Term && rf.state == Leader {
			if reply.Success {
				rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				// rf.slog("update %d matchIndex %d, nextIndex %d\n", i, rf.matchIndex[i], rf.nextIndex[i])
				rf.updateCommitIndex()
			} else if reply.Term > rf.currentTerm {
				// rf.slog("change to follower because AE reply from %d[%d], term %d -> %d\n", i, reply.Term, rf.currentTerm, reply.Term)
				rf.switchTerm(reply.Term)
				rf.switchState(Follower)
				rf.persist()
			} else if reply.Term == rf.currentTerm {
				rf.nextIndex[i] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					firstLogIndex := rf.firstLogIndex()
					for j := args.PrevLogIndex; j >= firstLogIndex; j-- {
						if rf.getLog(j).Term == reply.ConflictTerm {
							rf.nextIndex[i] = j + 1
							// rf.slog("update nextIndex[%d] to ConflictIndex %d\n", i, rf.nextIndex[i])
							break
						}
					}
				}
			}
		}
	}
}

func (rf *Raft) replicaTo(i int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	nextIndex := rf.nextIndex[i]
	if nextIndex < rf.firstLogIndex() {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.prevLogIndex(),
			LastIncludedTerm:  rf.prevLogTerm(),
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.RUnlock()
		rf.replicaByIS(i, args)
	} else {
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getLogTerm(nextIndex - 1),
			Entries:      rf.copyLogFrom(nextIndex),
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.RUnlock()
		rf.replicaByAE(i, args, nextIndex)
	}
}

func (rf *Raft) replicate(once bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if once {
			go rf.replicaTo(i)
		} else {
			rf.replicaCond[i].Signal()
		}
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.lastLogIndex()
}

// Replicator is a goroutine that replicates logs to peers by batch.
func (rf *Raft) replicator(i int) {
	rf.replicaCond[i].L.Lock()
	defer rf.replicaCond[i].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(i) {
			rf.replicaCond[i].Wait()
		}
		rf.replicaTo(i)
	}
}

func (rf *Raft) commiter() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.commitCond.Wait()
		}
		startIndex := rf.lastApplied + 1
		commitIndex := rf.commitIndex
		entries := rf.getLogSlice(startIndex, commitIndex+1)
		rf.mu.Unlock()

		for i, entry := range entries {
			// rf.slog("commit log[%d] %d\n", startIndex+i, entry.Command)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: startIndex + i}
		}

		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
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
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	// rf.slog("Start log %d = %d\n", rf.LastLogIndex(), command)
	rf.persist()
	rf.replicate(false)
	return rf.lastLogIndex(), rf.currentTerm, true
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
	// rf.slog("start election for %d\n", rf.currentTerm+1)
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.switchState(Candidate)
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	voteCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == Candidate {
					if reply.VoteGranted {
						voteCount++
						if voteCount > len(rf.peers)/2 {
							rf.switchState(Leader)
							rf.replicate(true)
						}
					} else if reply.Term > rf.currentTerm {
						// rf.slog("change to follower because receive RV reply from %d[%d], term %d -> %d\n", server, args.Term, rf.currentTerm, args.Term)
						rf.switchTerm(reply.Term)
						rf.switchState(Follower)
						rf.persist()
					}
				}
			}
		}(i)
	}
}

func makeTimeout() time.Duration {
	return time.Duration(ElectionMinTimeoutMs+rand.Intn(ElectionRandomTimeoutMs)) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.electionTimer.Reset(makeTimeout())
			rf.startElection()

		case <-rf.heartbeatTimer.C:
			rf.heartbeatTimer.Reset(HeartbeatIntervalMs * time.Millisecond)
			rf.replicate(true)
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
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.replicaCond = make([]*sync.Cond, len(peers))
	rf.electionTimer = time.NewTimer(makeTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartbeatIntervalMs * time.Millisecond)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // index 0 is dummy
	// log[0].Term is the term of the prev log entry
	// log[0].Command is index of the prev log entry
	rf.log[0] = LogEntry{Term: 0, Command: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// rf.slog("remake\n")

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.replicaCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commiter()

	return rf
}
