package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongConfig = "ErrWrongConfig"
	ErrTimeout	   = "ErrTimeout"
)

type Err string

type OpType int
type ShardState int
const (
	AppendOp 		OpType = 0
	PutOp			OpType = 1
	GetOp			OpType = 2
	ActivateOp		OpType = 3
	DeactivateOp 	OpType = 4
)

const (
	Serving 		ShardState = 0
	Offline			ShardState = 1
	Pulling 		ShardState = 2
	Serving 		ShardState = 3
)

type ShardStateMachine struct {
	Valid 		bool
	Version 	int
	Data		map[string]string
	ClientReq	map[int64]int
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   		string
	Value 		string
	Op    		OpType // "Put" or "Append"
	ClientId 	int64
	SeqNum 		int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err 		Err
	LeaderId 	int
}

type GetArgs struct {
	Key 		string
	ClientId 	int64
	SeqNum 		int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   		Err
	LeaderId 	int
	Value 		string
}

type ActivateArgs struct {
	Shard 		int
	Data 		[]byte
	ClientId 	int64
	SeqNum		int
	Version 	int
}

type DeactivateArgs struct {
	Shard		int
	Gid			int
	ClientId	int64
	SeqNum 		int
	Version 	int
}

type ActivateReply struct {
	Err 		Err
	LeaderId	int
}

type DeactivateReply struct {
	Err			Err
	LeaderId	int
}

