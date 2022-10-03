package kvraft

const (
	OK uint8 = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

const (
	GET uint8 = iota
	PUT
	APPEND
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    uint8 // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int64
}

type PutAppendReply struct {
	Err uint8
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	Seq      int64
}

type GetReply struct {
	Err   uint8
	Value string
}
