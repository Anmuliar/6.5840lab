package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "sync/atomic"
import "6.5840/labgob"
import "6.5840/shardctrler"
import "bytes"
import "log"
import "time"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation 	OpType 
	Key			string
	Value 		string
	Shard 		int
	Gid 		int
	ClientId	int64
	SeqNum		int
	Version 	int
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
	stateMachines 	[shardctrler.NShards]ShardStateMachine
	waitCh		 	map[int]chan OpResult

	// As a client to send the shard information
	seqNum 		int
	clientId 	int64
}

func(kv *ShardKV) Snapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachines)
	e.Encode(kv.lastApplied)

	kv.rf.Snapshot(kv.lastApplied, w.Bytes())
}
func(kv *ShardKV) InstallSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return 
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var statemachines  [shardctrler.NShards]ShardStateMachine
	var lastApplied	   int
	if d.Decode(&statemachines) != nil ||
	   d.Decode(&lastApplied) != nil{
		log.Printf("Failed to decode the snapshot!")
	} else {
		kv.stateMachines = statemachines
		kv.lastApplied = lastApplied
	}
}
func (kv *ShardKV) Submit(op Op) (Err, int, string) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, -1, ""
	}
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
			return committedOp.Err, kv.rf.GetLeader(), ""
		}
	case <- time.After(500 * time.Millisecond):
		return ErrTimeout, -1, ""
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
		SeqNum:		 args.SeqNum,
	}
	var _ string
	reply.Err,  reply.LeaderId, _ = kv.Submit(op) 

}
func (kv *ShardKV) Activate(args *ActivateArgs, reply *ActivateReply) {
	op := Op{
		Operation: ActivateOp,
		Shard:     args.Shard,
		ClientId:  args.ClientId,
		Data: 	   args.Data,
		SeqNum:    args.SeqNum,
		Version:   args.Version,
	}
	var _ string
	reply.Err, reply.LeaderId, _ = kv.Submit(op)
}

func (kv *ShardKV) Deactivate(args *DeactivateArgs, reply *DeactivateReply) {
	op := Op{
		Operation: DeactivateOp,
		Shard:     args.Shard,
		Gid:	   args.Gid,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
		Version:   args.Version,
	}
	var _ string
	reply.Err, reply.LeaderId, _ = kv.Submit(op)
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			log.Printf("%v-%v recieve msg %v from raft", kv.gid,kv.me, msg)
			// if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			// 	kv.Snapshot()
			// }
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				result := OpResult {
					Err: 			OK,
					SeqNum:			op.SeqNum,
					ClientId: 		op.ClientId,
				}
				lastSeq, flag := kv.stateMachines[op.Shard].ClientReq[op.ClientId]
				if !flag || op.SeqNum > lastSeq {
					switch op.Operation {
					case PutOp:
						if kv.stateMachines[op.Shard].Valid {
							kv.stateMachines[op.Shard].Data[op.Key] = op.Value
						} else {
							result.Err = ErrWrongGroup
						}
					case AppendOp:
						if kv.stateMachines[op.Shard].Valid {
							kv.stateMachines[op.Shard].Data[op.Key] += op.Value
						} else {
							result.Err = ErrWrongGroup
						}
					case GetOp:
						if kv.stateMachines[op.Shard].Valid {
							result.Value = kv.stateMachines[op.Shard].Data[op.Key]
							log.Printf("Value get on %v-%v is %v", kv.gid, kv.me, result.Value)
						} else {
							result.Err = ErrWrongGroup
						}
					case ActivateOp:
						if kv.stateMachines[op.Shard].Version >= op.Version  {
							result.Err = ErrWrongConfig
						} else {
							kv.stateMachines[op.Shard] = op.Data
							kv.stateMachines[op.Shard].Valid = true
						}
					case DeactivateOp:
						if kv.stateMachines[op.Shard].Version >= op.Version {
							result.Err = ErrWrongConfig
						} else {
							kv.stateMachines[op.Shard].Valid = false
							ssm := kv.stateMachines[op.Shard] // create a copy of the state machine
							go kv.ActivateClient(op.Shard, op.Gid ,ssm, op.Version) // use the copy in ActivateClient
						}
					}
					log.Printf("%v-%v serves %v:%v",kv.gid, kv.me ,op.Shard, kv.stateMachines[op.Shard])
					if kv.stateMachines[op.Shard].ClientReq == nil {
						kv.stateMachines[op.Shard].ClientReq = make(map[int64]int)
					}
					if kv.stateMachines[op.Shard].Data == nil {
						kv.stateMachines[op.Shard].Data = make(map[string]string)
					}
					kv.stateMachines[op.Shard].ClientReq[op.ClientId] = op.SeqNum
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
				kv.InstallSnapshot(msg.Snapshot)
				continue
			}
		}
	}
}
func (kv *ShardKV) DeactivateClient(shard int, tgtgid int, version int) {
	kv.seqNum ++
	
	args := DeactivateArgs{
		Shard: 		shard,
		Gid:		tgtgid,
		ClientId:	kv.clientId,
		SeqNum:		kv.seqNum,
		Version: 	version,
	}
	for !kv.killed() {
		if kv.config.Num > version {
			break
		}
		var reply DeactivateReply
		kv.Deactivate(&args, &reply)
		// log.Printf("Submit deactivate apply %v-%v shard:%v result:%v",kv.gid,kv.me,shard,reply)
		if reply.Err == OK {
			return 
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) ActivateClient(shard int, gid int, data ShardStateMachine, version int) {
	kv.seqNum ++
	args := ActivateArgs{
		Shard: 		shard,
		Data:		data,
		ClientId: 	kv.clientId,
		SeqNum:		kv.seqNum,
		Version: 	version,
	}
	if args.Data.ClientReq == nil || args.Data.Data == nil  {
		log.Printf("291 trying to activate a nil data. %v-%v",kv.gid,kv.me)
	}
	for !kv.killed() {
		if kv.config.Shards[shard] != gid {
			break
		}
		servers, ok := kv.config.Groups[gid]
		if ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply ActivateReply
				ok := srv.Call("ShardKV.Activate", &args, &reply)
				if ok && reply.Err == OK {
					return 
				}
				if ok && reply.Err == ErrWrongConfig {
					if kv.config.Num == version  {
						break
					} else {
						return
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) CompareConfig(newConfig shardctrler.Config) bool {
	// Compare the number of groups
	if len(kv.config.Groups) != len(newConfig.Groups) {
		return false
	}

	// Compare the shards assignment
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.config.Shards[i] != newConfig.Shards[i] {
			return false
		}
	}

	return true
}

func (kv *ShardKV) controler() {
	for !kv.killed() {
		config := kv.sm.Query(-1)
		oldconfig := kv.config
		if !kv.CompareConfig(config) {
			log.Printf("new config:%v old config:%v",config, oldconfig)
			kv.mu.Lock()
			kv.config = config
			for shard := 0; shard < shardctrler.NShards; shard++ {
				if oldconfig.Shards[shard] != kv.config.Shards[shard] {
					if oldconfig.Shards[shard] == 0 && kv.config.Shards[shard] == kv.gid {
						var ssm ShardStateMachine
						ssm.Valid = true
						ssm.Version = config.Num
						ssm.Data = make(map[string]string)
						ssm.ClientReq = make(map[int64]int)
						ssm.Data["test"] = "testtest"
						if ssm.Data == nil || ssm.ClientReq == nil {
							log.Printf("350 trying to activate a nil data. %v-%v",kv.gid,kv.me)
						}
						go kv.ActivateClient(shard, kv.gid, ssm, config.Num)	
					}
					if oldconfig.Shards[shard] == kv.gid {
						go kv.DeactivateClient(shard, kv.config.Shards[shard], config.Num)
					}
				}
			}
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
	log.Printf("%v-%v killed.",kv.gid,kv.me)
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
	kv.seqNum = 0
	kv.clientId = nrand()
	for i := 0; i < shardctrler.NShards; i++ {
		kv.stateMachines[i] = ShardStateMachine{
			Data:    make(map[string]string),
			Valid:   false,
			ClientReq: make(map[int64]int),
			Version: -1,
		}
	}
	kv.waitCh = make(map[int]chan OpResult)
	go kv.applier()
	go kv.controler()
	return kv
}
