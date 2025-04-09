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
func (kv *ShardKV) logcheck() {
	for shard := 0; shard < len(kv.stateMachines); shard++ {
		if kv.stateMachines[shard].Version <= kv.logNum {
			return
		}
	}
	log.Printf("%v-%v log updated to %v",kv.gid, kv.me, kv.logNum + 1)
	kv.logNum++
	kv.config = kv.sm.Query(kv.logNum)
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
							if kv.stateMachines[op.Shard].Valid == false {
								kv.DecodeSSM(op.Shard, op.Data)
								kv.stateMachines[op.Shard].Valid = true
							}
							kv.stateMachines[op.Shard].Version = op.Version
							log.Printf("%v-%v activated shard %v",kv.gid,kv.me,op.Shard)
							kv.logcheck()
						}
					case DeactivateOp:
						if kv.stateMachines[op.Shard].Version >= op.Version {
							result.Err = ErrWrongConfig
						} else {
							kv.stateMachines[op.Shard].Valid = false
							kv.stateMachines[op.Shard].Version = op.Version
							log.Printf("%v-%v deactivated shard %v",kv.gid,kv.me,op.Shard)
							kv.logcheck()
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
func (kv *ShardKV) DeactivateClient(shard int, version int) {
	kv.seqNum ++
	
	args := DeactivateArgs{
		Shard: 		shard,
		ClientId:	kv.clientId,
		SeqNum:		kv.seqNum,
		Version: 	version,
	}
	
	var reply DeactivateReply
	kv.Deactivate(&args, &reply)
	// log.Printf("Submit deactivate apply %v-%v shard:%v result:%v",kv.gid,kv.me,shard,reply)
	if reply.Err == OK {
		return 
	}
	if reply.Err == ErrWrongConfig {
		return 
	}
}
func (kv *ShardKV) ActivateClient(shard int, servers []string, data []byte, version int) {
	kv.seqNum ++
	args := ActivateArgs{
		Shard: 		shard,
		Data:		data,
		ClientId: 	kv.clientId,
		SeqNum:		kv.seqNum,
		Version: 	version,
	}
	for !kv.killed() {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply ActivateReply
			ok := srv.Call("ShardKV.Activate", &args, &reply)
			if ok && reply.Err == OK {
				log.Printf("%v-%v migrate %v done.",kv.gid,kv.me,args.Data)
				return 
			}
			if ok && reply.Err == ErrWrongConfig {
				log.Printf("%v-%v migrate %v fail due to outdate.",kv.gid,kv.me,args.Data)
				return
			}
		}
	
		time.Sleep(100 * time.Millisecond)
	}
}


func (kv *ShardKV) controler() {
	for !kv.killed() {
		config := kv.sm.Query(-1)
		for config.Num > kv.logNum {

			updconfig := kv.sm.Query(kv.logNum + 1)
			nowconfig := kv.config
			preconfig := kv.sm.Query(kv.logNum - 1)
			log.Printf("%v-%v trying to update the config%v to new config%v",kv.gid, kv.me, nowconfig, updconfig)
			for shard := 0; shard < shardctrler.NShards; shard ++ {
				if updconfig.Shards[shard] != kv.gid {
					kv.DeactivateClient(shard, updconfig.Num)
				} else {
					if nowconfig.Shards[shard] == 0 && nowconfig.Num == 0 {
						ssm := ShardStateMachine {
							Valid: 		true,
							Version: 	updconfig.Num,
							Data:		make(map[string]string),
							ClientReq:  make(map[int64]int),
						}
						ssmdata := kv.EncodeSSM(ssm)
						kv.ActivateClient(shard, updconfig.Groups[kv.gid], ssmdata, updconfig.Num)
					}
				}
				if nowconfig.Shards[shard] == 0 && nowconfig.Num != 0 {
					if preconfig.Shards[shard] == kv.gid && kv.stateMachines[shard].Valid == false{
						kv.mu.Lock()
						ssmdata := kv.EncodeSSM(kv.stateMachines[shard])
						kv.mu.Unlock()
						kv.ActivateClient(shard, updconfig.Groups[updconfig.Shards[shard]], ssmdata, updconfig.Num)
					}
				}
				if nowconfig.Shards[shard] == kv.gid && kv.stateMachines[shard].Valid == false{
					kv.mu.Lock()
					ssmdata := kv.EncodeSSM(kv.stateMachines[shard])
					kv.mu.Unlock()
					kv.ActivateClient(shard, updconfig.Groups[updconfig.Shards[shard]], ssmdata, updconfig.Num)
				}
			}
			log.Printf("%v-%v migrate log to %v",kv.gid, kv.me, kv.logNum)
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
	kv.seqNum = 0
	kv.logNum = 0 
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.InstallSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	
	go kv.applier()
	go kv.controler()
	return kv
}
