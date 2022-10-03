package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func assert(cond bool) {
	if Debug && !cond {
		panic("bug")
	}
}

const ExecuteTimeout = 500 * time.Millisecond

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int64
	Op       uint8
	Key      string
	Value    string
}

type CommandResult struct {
	Err   uint8
	Value string
}

type LastCommandRes struct {
	Seq int64
	Res *CommandResult
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	m        map[string]string
	notifyCh map[int]chan *CommandResult
	lastCmd  map[int64]LastCommandRes
}

func (kv *KVServer) getNotifyCh(index int) chan *CommandResult {
	if _, ok := kv.notifyCh[index]; !ok {
		kv.notifyCh[index] = make(chan *CommandResult, 1)
	}
	return kv.notifyCh[index]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.RLock()
	if lastCmd, ok := kv.lastCmd[args.ClientId]; ok && lastCmd.Seq == args.Seq {
		DPrintf("%d %d %d Get \"%s\": \"%s\", Err %d, duplicated", kv.me, args.ClientId, args.Seq, args.Key, reply.Value, reply.Err)
		reply.Err = lastCmd.Res.Err
		reply.Value = lastCmd.Res.Value
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, ok := kv.rf.Start(Command{Op: GET, Key: args.Key, ClientId: args.ClientId, Seq: args.Seq})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case opRes := <-notifyCh:
		reply.Err = opRes.Err
		reply.Value = opRes.Value
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	DPrintf("%d %d %d Get \"%s\": \"%s\", Err %d", kv.me, args.ClientId, args.Seq, args.Key, reply.Value, reply.Err)
	go func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.RLock()
	if lastCmd, ok := kv.lastCmd[args.ClientId]; ok && lastCmd.Seq == args.Seq {
		DPrintf("%d %d %d %s \"%s\" \"%s\": %d, duplicated", kv.me, args.ClientId, args.Seq, []string{"Get", "Put", "Append"}[args.Op], args.Key, args.Value, reply.Err)
		reply.Err = lastCmd.Res.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, ok := kv.rf.Start(Command{Op: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, Seq: args.Seq})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case opRes := <-notifyCh:
		reply.Err = opRes.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	DPrintf("%d %d %d %s \"%s\" \"%s\": %d", kv.me, args.ClientId, args.Seq, []string{"Get", "Put", "Append"}[args.Op], args.Key, args.Value, reply.Err)
	go func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) execute(op *Command) *CommandResult {
	opRes := &CommandResult{Err: OK}
	switch op.Op {
	case GET:
		value, ok := kv.m[op.Key]
		if !ok {
			opRes.Err = ErrNoKey
		} else {
			opRes.Value = value
		}
	case PUT:
		kv.m[op.Key] = op.Value
	case APPEND:
		kv.m[op.Key] += op.Value
	}
	return opRes
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		assert(msg.CommandValid || msg.SnapshotValid)
		if msg.CommandValid {
			kv.mu.Lock()
			cmd := msg.Command.(Command)
			var cmdRes *CommandResult

			if lastCmd, ok := kv.lastCmd[cmd.ClientId]; ok && lastCmd.Seq == cmd.Seq {
				cmdRes = lastCmd.Res
			} else {
				cmdRes = kv.execute(&cmd)
				kv.lastCmd[cmd.ClientId] = LastCommandRes{Seq: cmd.Seq, Res: cmdRes}
			}

			if _, leader := kv.rf.GetState(); leader {
				notifyCh := kv.getNotifyCh(msg.CommandIndex)
				notifyCh <- cmdRes
			}

			if needSnapshot, snapshot := kv.needSnapshot(); needSnapshot {
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}

			kv.mu.Unlock()
		} else {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.loadSnapshot(msg.Snapshot)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) needSnapshot() (bool, []byte) {
	ok := kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate
	var snapshot []byte
	if ok {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.m)
		e.Encode(kv.lastCmd)
		snapshot = w.Bytes()
	}
	return ok, snapshot
}

func (kv *KVServer) loadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var m map[string]string
	var lastCmd map[int64]LastCommandRes
	if d.Decode(&m) != nil || d.Decode(&lastCmd) != nil {
		log.Fatal("decode snapshot error")
	} else {
		kv.m = m
		kv.lastCmd = lastCmd
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.m = make(map[string]string)
	kv.notifyCh = make(map[int]chan *CommandResult)
	kv.lastCmd = make(map[int64]LastCommandRes)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.loadSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applier()

	return kv
}
