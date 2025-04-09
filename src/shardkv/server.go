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
	ClientId	int64
	SeqNum		int
	Version 	int
	Config 		shardctrler.Config
	PreConfig   shardctrler.Config
	Data 		[]byte
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
	preconfig    shardctrler.Config
	sm 			 *shardctrler.Clerk
	dead		 int32 
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	lastApplied  int
	logNum 		 int
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
	e.Encode(kv.logNum)

	kv.rf.Snapshot(kv.lastApplied, w.Bytes())
}
func(kv *ShardKV) EncodeSSM(ssm ShardStateMachine) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(ssm)
	return w.Bytes()
}
func(kv *ShardKV) DecodeSSM(shard int, data []byte) {

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ssm ShardStateMachine
	if d.Decode(&ssm) != nil {

	} else {
		kv.stateMachines[shard] = ssm
	} 
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
	var logNum 		   int
	if d.Decode(&statemachines) != nil ||
	   d.Decode(&lastApplied) != nil ||
	   d.Decode(&logNum) != nil{
		log.Printf("Failed to decode the snapshot!")
	} else {
		kv.stateMachines = statemachines
		kv.lastApplied = lastApplied
		kv.logNum = logNum
		log.Printf("statemachine decode:%v",statemachines)
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
func (kv *ShardKV) PullData(args *PullDataArgs, reply *PullDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Version == kv.config.Num && kv.stateMachines[args.Shard].State == Erasing {
		reply.Data = kv.EncodeSSM(kv.stateMachines[args.Shard])
		reply.Err = OK
	} 
}
func (kv *ShardKV) EraseData(args *EraseDataArgs, reply *EraseDataReply) {

	if args.Version == kv.config.Num && kv.stateMachines[args.Shard].State == Erasing {
		kv.mu.Lock()
		kv.seqNum ++
		op := Op {
			Operation:   EraseOp,
			Shard: 		 args.Shard,
			Version:	 args.Version,
			ClientId: 	 kv.clientId,
			SeqNum: 	 args.seqNum,
		}
		kv.mu.Unlock()
		reply.Err, _, _ = kv.Submit(op)
	}

}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			log.Printf("%v-%v recieve msg %v from raft", kv.gid,kv.me, msg)
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
				lastSeq, flag := kv.stateMachines[op.Shard].ClientReq[op.ClientId]
				if !flag || op.SeqNum > lastSeq {
					switch op.Operation {
					case PutOp:
						if kv.stateMachines[op.Shard].State == Serving {
							kv.stateMachines[op.Shard].Data[op.Key] = op.Value
						} else {
							result.Err = ErrWrongGroup
						}
					case AppendOp:
						if kv.stateMachines[op.Shard].State == Serving {
							kv.stateMachines[op.Shard].Data[op.Key] += op.Value
						} else {
							result.Err = ErrWrongGroup
						}
					case GetOp:
						if kv.stateMachines[op.Shard].State == Serving {
							result.Value = kv.stateMachines[op.Shard].Data[op.Key]
							log.Printf("Value get on %v-%v is %v", kv.gid, kv.me, result.Value)
						} else {
							result.Err = ErrWrongGroup
						}
					case UpdateOp:
						if op.Version <= kv.logNum {
							result.Err = ErrWrongConfig
						} else {
							for shard := 0; shard < 10; shard ++ {
								if op.Config.Shards[shard] == kv.gid {
									if op.PreConfig.Shards[shard] != kv.gid && op.Config.Num != 1{
										kv.stateMachines[shard].State = Pulling
									} else {
										kv.stateMachines[shard].State = Serving
									}
								} else {
									if op.PreConfig.Shards[shard] == kv.gid {
										kv.stateMachines[shard].State = Erasing
									} else {
										kv.stateMachines[shard].State = Offline
									}
								}
							}
							kv.logNum = op.Version
							kv.config = op.Config
							kv.preconfig = op.PreConfig
						}
					case ActivateOp:
						if op.Version != kv.logNum && kv.stateMachines[shard].State != Pulling{
							result.Err = ErrWrongConfig
						} else {
							kv.DecodeSSM(op.Shard, op.Data)
							kv.stateMachines[shard].State = Waiting
						}
					case EraseOp:
						if op.Version != kv.logNum && kv.stateMachines[shard].State != Erasing{
							result.Err = ErrWrongConfig
						} else {
							kv.stateMachines[shard].State = Offline
						}
					case OnlineOp:
						if op.Version != kv.logNum && kv.stateMachines[shard].State != Waiting{
							result.Err = ErrWrongConfig
						} else {
							kv.stateMachines[shard].State = Serving
						}
					// log.Printf("%v-%v serves %v:%v",kv.gid, kv.me ,op.Shard, kv.stateMachines[op.Shard])
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



// func (kv *ShardKV) controler() {
// 	for !kv.killed() {
// 		config := kv.sm.Query(-1)
// 		for config.Num > kv.logNum {

// 			updconfig := kv.sm.Query(kv.logNum + 1)
// 			nowconfig := kv.config
// 			preconfig := kv.sm.Query(kv.logNum - 1)
// 			log.Printf("%v-%v trying to update the config%v to new config%v",kv.gid, kv.me, nowconfig, updconfig)
// 			for shard := 0; shard < shardctrler.NShards; shard ++ {
// 				if updconfig.Shards[shard] != kv.gid {
// 					kv.DeactivateClient(shard, updconfig.Num)
// 				} else {
// 					if nowconfig.Shards[shard] == 0 && nowconfig.Num == 0 {
// 						ssm := ShardStateMachine {
// 							Valid: 		true,
// 							Version: 	updconfig.Num,
// 							Data:		make(map[string]string),
// 							ClientReq:  make(map[int64]int),
// 						}
// 						ssmdata := kv.EncodeSSM(ssm)
// 						kv.ActivateClient(shard, updconfig.Groups[kv.gid], ssmdata, updconfig.Num)
// 					}
// 				}
// 				if nowconfig.Shards[shard] == 0 && nowconfig.Num != 0 {
// 					if preconfig.Shards[shard] == kv.gid && kv.stateMachines[shard].Valid == false{
// 						kv.mu.Lock()
// 						ssmdata := kv.EncodeSSM(kv.stateMachines[shard])
// 						kv.mu.Unlock()
// 						kv.ActivateClient(shard, updconfig.Groups[updconfig.Shards[shard]], ssmdata, updconfig.Num)
// 					}
// 				}
// 				if nowconfig.Shards[shard] == kv.gid && kv.stateMachines[shard].Valid == false{
// 					kv.mu.Lock()
// 					ssmdata := kv.EncodeSSM(kv.stateMachines[shard])
// 					kv.mu.Unlock()
// 					kv.ActivateClient(shard, updconfig.Groups[updconfig.Shards[shard]], ssmdata, updconfig.Num)
// 				}
// 			}
// 			log.Printf("%v-%v migrate log to %v",kv.gid, kv.me, kv.logNum)
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }
func (kv *ShardKB) canupdate() bool {
	for shard := 0; shard < 10 ; shard ++ {
		if kv.stateMachines[shard] != Serving || kv.stateMachines[shard] != Offline {
			return false
		}
	}
	return true
}
func (kv *ShardKV) controler() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		_, isleader = kv.rf.GetState()
		
		if !isleader {
			continue
		}
		if kv.canupdate() {
			config := kv.sm.Query(kv.logNum + 1)
			if config.Num == kv.logNum + 1 {
				kv.mu.Lock()
				kv.seqNum ++
				subOp := Op{
					Operation: 		UpdateOp,
					Config: 		config,	
					Version: 		config.Num,
					PreConfig:		kv.config,
					ClientId:	 	kv.clientId,
					SeqNum:			kv.seqNum, 		
				}
				kv.mu.Unlock()
				kv.Submit(subOp)
			}
		}
	}
}
func (kv *ShardKV) datapuller() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		_, isleader = kv.rf.GetState()
		if !isleader {
			continue
		}
		for shard := 0; shard < 10; shard ++ {
			if kv.stateMachines[shard] == Pulling {
				gid := kv.preconfig.Shards[shard]
				if servers, ok := kv.preconfig.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply PullDataReply 
						args := PullDataArgs{
							Version: 	config.Num,
							Shard: 		shard,
						}
						ok := srv.Call("Shard.PullData", &args, &reply)
						if ok && reply.Err == OK {
							kv.mu.Lock()
							kv.seqNum ++
							subOp := Op{
								Operation: 		ActivateOp,
								Version: 		config.Num,
								Data: 			reply.Data,
								Shard: 			shard,
								ClientId: 		kv.clientId,
								SeqNum: 		kv.seqNum,
							}
							kv.mu.Unlock()
							ok,_,_ := kv.Submit(subOp)
							if ok {
								break
							}
						}
					}
				}
			}
		}
	}
}
func (kv *ShardKV) dataeraser() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		_, isleader = kv.rf.GetState()
		if !isleader {
			continue
		}
		for shard := 0; shard < 10; shard ++ {
			if kv.stateMachines[shard] == Waiting {
				gid := kv.preconfig.Shards[shard]
				if servers, ok := kv.preconfig.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply EraseDataReply
						args := EraseDataArgs {
							Version: 		config.Num,
							Shard: 			shard,
						}
						ok := srv.Call("Shard.EraseData", &args, &reply)
						if ok && reply.Err == OK {
							kv.mu.Lock()
							kv.seqNum ++
							subOp := Op{
								Operation: 	OnlineOp,
								Version: 	config.Num,
								Shard: 		shard,
								ClientId: 	kv.clientId,
								SeqNum: 	kv.seqNum,	
							}
							kv.mu.Unlock()
							ok,_,_ := kv.Submit(subOp)
							if ok {
								break
							}
						}
					}
				}
			}
		}
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
	kv.seqNum = 0
	kv.logNum = 0 
	kv.clientId = nrand()
	for i := 0; i < shardctrler.NShards; i++ {
		kv.stateMachines[i] = ShardStateMachine{
			Data:    make(map[string]string),
			State:   Offline,
			ClientReq: make(map[int64]int),
		}
	}
	kv.config = kv.sm.Query(0)
	kv.waitCh = make(map[int]chan OpResult)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.InstallSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	
	go kv.applier()
	go kv.controler()
	return kv
}
