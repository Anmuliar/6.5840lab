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

const (
	AppendOp 		OpType = 0
	PutOp			OpType = 1
	GetOp			OpType = 2
	UpdateOp		OpType = 3
	ActivateOp 		OpType = 4
	EraseOp			OpType = 5
	OnlineOp 		OpType = 6
)
type ShardState int

const (
	Serving 		ShardState = 0
	Offline			ShardState = 1
	Pulling 		ShardState = 2
	Waiting        	ShardState = 3
	Erasing 		ShardState = 4
)
type RequestReply struct {
	SeqNum 		int
	Value 		string
}

type ShardStateMachine struct {
	State 		ShardState
	Data		map[string]string
	ClientReq	map[int64]RequestReply
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

type PullDataArgs struct{
	Version 	int
	Shard 		int
}

type EraseDataArgs struct {
	Version 	int
	Shard 		int
}

type PullDataReply struct {
	Err			Err
	Data 		[]byte
}

type EraseDataReply struct {
	Err			Err
}