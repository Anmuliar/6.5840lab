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
)

type Err string
type OpType int 
const {
	AppendOp 		OpType = 0
	PutOp			OpType = 1
	GetOp			OpType = 2
	ActivateOp		OpType = 3
	DeactivateOp 	OpType = 4
}

type ShardStateMachine struct {
	valid 		bool
	data		map[string]string
	clientReq	map[int64]int
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
	Data 		ShardStateMachine
	ClientId 	int64
	SeqNum		int
}

type DeactivateArgs struct {
	Shard		int
	ClientId	int64
	SeqNum 		int
}

type ActivateReply struct {
	Err 		Err
	LeaderId	int
}

type DeactivateReply struct {
	Err			Err
	LeaderId	int
}

