package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation 	OpType 
	Key			string
	Value 		string
	Shard 		int
	ClientId	int64
	SeqNum		int
	Data 		ShardStateMachine
}


type OpResult struct {
	Err 		Err
	Value 		string
	LeaderId 	int
	ClientId	int64
	SeqNum		int
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	config 		 shardctrler.Config
	sm 			 *shardctrler.Clerk
	dead		 int32 
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	lastApplied  int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister 	 *raft.Persister

	// Your definitions here.
	stateMachines 	map[int]ShardStateMachine
	waitCh		 	map[int]chan OpResult
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func(kv *KVServer) Snapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachines)
	e.Encode(kv.lastApplied)

	kv.rf.Sanpshot(kv.lastApplied, w.Bytes())
}
func(kv *KVServer) InstallSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return 
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var statemachines  map[int]ShardStateMachine
	var lastApplied	   int
	if d.Decode(&statemachine) != nil ||
	   d.Decode(&lastApplied) != nil{
		log.Printf("Failed to decode the snapshot!")
	} else {
		kv.stateMachines = statemachines
		kv.lastApplied = lastApplied
	}
}
func (sc *ShardCtrler) Submit(op Op) (Err, int, string) {
	index, _, isLeader := kv.rf.Start(op)
	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.waitCh[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
	}()
	select {
	case committedOp := <-ch:
		if committedOp.SeqNum == op.SeqNum {
			return committedOp.Err, committedOp.LeaderId, committedOp.Value 
		} else {
			return committedOp.Err, sc.rf.GetLeader(), string{}
		}
	case <- time.After(500 * time.Millisecond):
		return ErrTimeout, -1, string{}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op {
		Operation:  GetOp,
		Key: 		args.Key,
		Shard: 		key2shard(args.Key),
		ClientId:	args.ClientId,
		SeqNum:		args.SeqNum,
	}
	reply.Err, reply.LeaderId, reply.Value = kv.Submit(op)
	
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation:	 args.Op, 
		Key: 		 args.Key, 
		Value:		 args.Value, 
		Shard: 		 key2shard(args.Key),
		ClientId: 	 args.ClientId, 
		SeqNum:		 args.SeqNum
	}
	var _ string
	reply.Err,  reply.LeaderId, _ = kv.Submit(op) 

}
func (kv *ShardKV) Activate(args *ActivateArgs, reply *ActivateReply) {
	op := Op{
		Operation: ActivateOp,
		Shard:     args.Shard,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}
	var _ string
	reply.Err, reply.LeaderId, _ = kv.Submit(op)
}

func (kv *ShardKV) Deactivate(args *DeactivateArgs, reply *DeactivateReply) {
	op := Op{
		Operation: DeactivateOp,
		Shard:     args.Shard,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}
	var _ string
	reply.Err, reply.LeaderId, _ = kv.Submit(op)
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.Snapshot()
			}
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				result := OpResult {
					Err: 			OK,
					SeqNum:			op.SeqNum,
					ClientId: 		op.ClientId,
				}
				lastSeq, flag := kv.stateMachines[op.Shard].clientReq[op.ClientId]
				if !flag || op.SeqNum > lastSeq {
					switch op.Opeartion {
					case PutOp:
						if kv.stateMachines[op.Shard].valid {
							kv.stateMachines[op.Shard].data[op.Key] = op.Value
						} else {
							result.Err = ErrWrongGroup
						}
					case AppendOp:
						if kv.stateMachines[op.Shard].valid {
							kv.stateMachines[op.Shard].data[op.Key] += op.Value
						} else {
							result.Err = ErrWrongGroup
						}
					case GetOp:
						if kv.stateMachines[op.Shard].valid {
							result.Value = kv.stateMachines[op.Shard].data[op.Key]
						} else {
							result.Err = ErrWrongGroup
						}
					case ActivateOp:
						kv.stateMachines[op.Shard] = op.Data
						kv.stateMachines[op.Shard].valid = true
					case DeactivateOp:
						kv.stateMachines[op.Shard].valid = false
					}
					kv.stateMachines[op.Shard].clientReq[op.ClientId] = op.SeqNum
				}
				if msg.CommandIndex > kv.lastApplied {
					kv.lastApplied = msg.CommandIndex 
				}
				ch, ok := kv.waitCh[msg.CommandIndex]
				if ok {
					_, isLeader := kv.rf.GetState()
					if !isLeader {
						result.Err = ErrWrongLeader
						result.LeaderId = kv.rf.GetLeader()
					}
					ch <- result 
				}
				kv.mu.Unlock()
			}
			if msg.SnapshotValid {
				kv.InstallSnapshot(msg.snapshots)
				continue
			}
		}
	}
}
func (kv *KVServer) DeactivateClient(shard int) {

}
func (kv *KVServer) ActivateClient(shard int, data ShardStateMachine) {

}
func (kv *KVServer) controler() {
	for !kv.killed() {
		config = kv.sm.Query(-1)
		if config != kv.config {
			kv.mu.Lock()
			for shard := 0; shard < shardctrler.NShards; shard++ {
				if config.Shards[shard] != kv.config.Shards[shard] {
					if kv.config.Shards[shard] == 0 && config.Shards[shard] == kv.gid {
						var ssm ShardStateMachine
						ssm.valid = true
						ssm.data = make(map[string]string)
						ssm.clientReq = make(map[int64]int)
						ActivateClient(shard,ssm)
					}
					if kv.config.Shards[shard] == kv.gid {
						DeactivateClient(shard)
					}
				}
			}
			kv.config = config
			kv.mu.Unlock()
		} 
		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.sm = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	shardctrler.NShards
	go kv.applier()
	return kv
}
