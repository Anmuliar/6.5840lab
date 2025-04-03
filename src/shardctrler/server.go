package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "sync/atomic"
import "6.5840/labgob"
import "time"
// import "log"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	dead 	int32
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs 	[]Config // indexed by config num
	clientReq	map[int64]int
	waitCh 		map[int]chan OpResult
}


type Op struct {
	Operation 		OpType
	Int1			int
	Int2			int
	IntArray		[]int
	Servers			map[int][]string
	ClientId		int64
	SeqNum			int
}

type OpResult struct {
	Err 			Err
	Value			Config
	LeaderId 		int
	ClientId		int64
	SeqNum			int
}
func (sc *ShardCtrler) Submit(op Op) (Err, int, Config) {

	// log.Printf("Submitting op %v",op)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, -1, Config{}
	}
	ch := make(chan OpResult, 1)
	sc.mu.Lock()
	sc.waitCh[index] = ch
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.waitCh, index)
		sc.mu.Unlock()
	}()
	select {
	case committedOp := <- ch:
		if committedOp.SeqNum == op.SeqNum {
			return committedOp.Err, committedOp.LeaderId, committedOp.Value
		} else {
			return committedOp.Err, sc.rf.GetLeader(), Config{}
		}
	case <- time.After(500 * time.Millisecond) :
		return ErrTimeout, -1, Config{}
	}
} 

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op {
		Operation:		JoinOp,
		Servers:		args.Servers,
		ClientId:		args.ClientId,
		SeqNum:			args.SeqNum,
	}
	var _ Config
	reply.Err, reply.LeaderId, _ = sc.Submit(op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op {
		Operation:		LeaveOp,
		IntArray:		args.GIDs,
		ClientId:		args.ClientId,
		SeqNum:			args.SeqNum,
	}
	var _ Config
	reply.Err, reply.LeaderId, _ = sc.Submit(op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op {
		Operation:		MoveOp,
		Int1:			args.Shard,
		Int2:			args.GID,
		ClientId:		args.ClientId,
		SeqNum:			args.SeqNum,
	}
	var _ Config
	reply.Err, reply.LeaderId, _ = sc.Submit(op)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op {
		Operation: 		QueryOp,
		Int1:			args.Num,
		ClientId:		args.ClientId,
		SeqNum:			args.SeqNum,
	}
	reply.Err, reply.LeaderId, reply.Config = sc.Submit(op)
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <- sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock() 
				op := msg.Command.(Op)
				result := OpResult {
					Err: 		OK,
					SeqNum:		op.SeqNum,
					ClientId: 	op.ClientId,
				}
				lastSeq, flag := sc.clientReq[op.ClientId]
				if !flag || op.SeqNum > lastSeq {
					switch op.Operation {
					case JoinOp:
						newConfig := Config{
							Num:    len(sc.configs),
							Shards: sc.configs[len(sc.configs)-1].Shards,
							Groups: make(map[int][]string),
						}
						// Copy existing groups
						for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
							newConfig.Groups[gid] = servers
						}
						// Add new groups
						for gid, servers := range op.Servers {
							newConfig.Groups[gid] = servers
						}
						// Assign shards to new groups
						// log.Printf("NewConfig: %v",newConfig)
						sc.configs = append(sc.configs, newConfig)
						sc.clientReq[op.ClientId] = op.SeqNum
					case LeaveOp:
						newConfig := Config{
							Num:	len(sc.configs),
							Shards: sc.configs[len(sc.configs)-1].Shards,
							Groups:	make(map[int][]string),
						}
						// Copy existing groups except those being removed
						for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
							removed := false
							for _, leaveGid := range op.IntArray {
								if gid == leaveGid {
									removed = true
									break
								}
							}
							if !removed {
								newConfig.Groups[gid] = servers
							}
						}
						
						// Reassign shards from removed groups to 0
						for shard := range newConfig.Shards {
							for _, leaveGid := range op.IntArray {
								if newConfig.Shards[shard] == leaveGid {
									newConfig.Shards[shard] = 0
									break
								}
							}
						}
						
						// Balance remaining shards
						// log.Printf("NewConfig: %v",newConfig)
						// newConfig.ShardBalance()
						sc.configs = append(sc.configs, newConfig)
						sc.clientReq[op.ClientId] = op.SeqNum
					case MoveOp:
						newConfig := Config{
							Num:	len(sc.configs),
							Shards: sc.configs[len(sc.configs)-1].Shards,
							Groups: sc.configs[len(sc.configs)-1].Groups,
						}
						newConfig.Shards[op.Int1] = op.Int2
						// log.Printf("NewConfig: %v",newConfig)
						sc.configs = append(sc.configs, newConfig)
						sc.clientReq[op.ClientId] = op.SeqNum
					case QueryOp:
						if op.Int1 > len(sc.configs) {
							result.Err = ErrNoKey
						}
						
						result.Value = sc.configs[(op.Int1 + len(sc.configs)) % len(sc.configs)]
						// log.Printf("In server %v: the No.%v config before balance is %v", sc.me, op.Int1, result.Value)
						result.Value.ShardBalance()
						// log.Printf("In server %v: the No.%v config after balance is %v", sc.me, op.Int1, result.Value)
					}
				}
				ch, ok := sc.waitCh[msg.CommandIndex]
				if ok {
					_, isLeader := sc.rf.GetState()
					if !isLeader {
						result.Err = ErrWrongLeader
						result.LeaderId = sc.rf.GetLeader()
					}
					ch <- result
				}
				sc.mu.Unlock()
			}
		}
	}
}
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}
// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.clientReq = make(map[int64]int)
	sc.waitCh = make(map[int]chan OpResult)
	// Your code here.
	go sc.applier()

	return sc
}
