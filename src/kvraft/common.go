package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout 	   = "ErrTimeout"
)

type Err string
type OpType int 
const (
	AppendOp 	OpType = 0
	PutOp		OpType = 1
	GetOp 		OpType = 2
)
// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		OpType // "Put" or "Append"
	ClientId  	int64
	SeqNum		int
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
	SeqNum		int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   		Err
	LeaderId 	int
	Value 		string
}
