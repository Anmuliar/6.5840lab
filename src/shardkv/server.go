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
import "fmt"

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
	clientId 	int64
}

func(kv *ShardKV) Snapshot() {
	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplied)
	e.Encode(kv.stateMachines)
	e.Encode(kv.logNum)
	e.Encode(kv.config)
	e.Encode(kv.preconfig)
	log.Printf("%v-%v snapshot %v statemachine:%v\n config:%v oldconfig:%v",kv.gid, kv.me, kv.lastApplied, kv.stateMachines,kv.config.Num,kv.preconfig.Num)
	index := kv.lastApplied
	bytes := make([]byte, len(w.Bytes()))
	copy(bytes, w.Bytes())
	log.Printf("%v-%v snapshot %v copy done",kv.gid, kv.me, kv.lastApplied)
	kv.mu.Unlock()
	kv.rf.Snapshot(index, bytes)
	log.Printf("%v-%v snapshot done.",kv.gid, kv.me)
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
	var lastApplied	   int
	var statemachines  [shardctrler.NShards]ShardStateMachine
	var logNum 		   int
	var config 		   shardctrler.Config
	var preconfig 	   shardctrler.Config
	if d.Decode(&lastApplied) != nil ||
	   d.Decode(&statemachines) != nil ||
	   d.Decode(&logNum) != nil ||
	   d.Decode(&config) != nil ||
	   d.Decode(&preconfig) != nil{
		log.Printf("Failed to decode the snapshot!")
	} else {
		kv.stateMachines = statemachines
		kv.lastApplied = lastApplied
		kv.logNum = logNum
		kv.config = config
		kv.preconfig = preconfig
		log.Printf("%v-%v statemachine decode:%v\nconfig: %v preconfig: %v",kv.gid, kv.me, statemachines, kv.config.Num,kv.preconfig.Num)
	}
}
func (kv *ShardKV) Submit(op Op) (Err, int, string) { 
	
	ch := make(chan OpResult, 1)
	index, _, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	
	kv.waitCh[index] = ch
	kv.mu.Unlock()
	log.Printf("%v-%v submit %v on index %v", kv.gid, kv.me, op, index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
	}()
	if !isLeader {
		return ErrWrongLeader, -1, ""
	}
	select {
	case committedOp := <-ch:
		log.Printf("%v-%v recieved reply on index %v with %v", kv.gid, kv.me, index, committedOp)
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
func (kv *ShardKV) CheckConfig(shard int, version int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if version == kv.config.Num && kv.stateMachines[shard].State == Erasing {
		return true
	}
	return false
}
func (kv *ShardKV) PullData(args *PullDataArgs, reply *PullDataReply) {

	log.Printf("[Server.pulldata]%v-%v recieve args:%v, current config is %v",kv.gid,kv.me,args,kv.config.Num)
	if kv.CheckConfig(args.Shard, args.Version) {
		kv.mu.Lock()
		reply.Data = kv.EncodeSSM(kv.stateMachines[args.Shard])
		reply.Err = OK
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongConfig
	}
}
func (kv *ShardKV) EraseData(args *EraseDataArgs, reply *EraseDataReply) {
	log.Printf("[Server.erasedata]%v-%v recieve args:%v, current config is %v",kv.gid,kv.me,args,kv.config.Num)
	kv.mu.Lock()
	op := Op {
		Operation:   EraseOp,
		Shard: 		 args.Shard,
		Version:	 args.Version,
		ClientId: 	 kv.clientId + int64(args.Shard * 100000000 + 50000000),
		SeqNum: 	 args.Version,
	}
	kv.mu.Unlock()
	reply.Err, _, _ = kv.Submit(op)
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		log.Printf("[Server.applier]%v-%v is waiting for msg.",kv.gid, kv.me)
		select {
		case msg := <- kv.applyCh:
			// log.Printf("%v-%v recieve msg %v from raft", kv.gid,kv.me, msg)
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
						log.Printf("[Server.Update]%v-%v trying to put %v to key %v on shard %v",kv.gid,kv.me, op.Value ,op.Key, op.Shard)
						if kv.stateMachines[op.Shard].State == Serving {
							kv.stateMachines[op.Shard].Data[op.Key] = op.Value
						} else {
							result.Err = ErrWrongGroup
						}
					case AppendOp:
						log.Printf("[Server.Append]%v-%v trying to append %v to key %v on shard %v client:%v seqnum:%v",kv.gid,kv.me, op.Value ,op.Key, op.Shard, op.ClientId, op.SeqNum)
						if kv.stateMachines[op.Shard].State == Serving {
							kv.stateMachines[op.Shard].Data[op.Key] += op.Value
						} else {
							result.Err = ErrWrongGroup
						}
					case GetOp:
						log.Printf("[Server.Get]%v-%v trying to get %v on shard %v",kv.gid,kv.me, op.Key, op.Shard)
						if kv.stateMachines[op.Shard].State == Serving {
							result.Value = kv.stateMachines[op.Shard].Data[op.Key]
							log.Printf("Value get on %v-%v is %v", kv.gid, kv.me, result.Value)
						} else {
							log.Printf("%v-%v Assign err.",kv.gid,kv.me)
							result.Err = ErrWrongGroup
						}
					case UpdateOp:
						log.Printf("[Server.Update]%v-%v trying to update from config %v\n to new config %v\n",kv.gid,kv.me,op.PreConfig.Num,op.Config.Num)
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
								log.Printf("%v-%v shard %v state is %v", kv.gid, kv.me, shard, kv.stateMachines[shard].State)
							}
							kv.logNum = op.Version
							kv.config = op.Config
							kv.preconfig = op.PreConfig
						}
					case ActivateOp:
						log.Printf("[Server.Activate]%v-%v trying to activate shard %v",kv.gid,kv.me, op.Shard)
						if op.Version != kv.logNum || kv.stateMachines[op.Shard].State != Pulling{
							result.Err = ErrWrongConfig
						} else {
							kv.DecodeSSM(op.Shard, op.Data)
							kv.stateMachines[op.Shard].State = Waiting
						}
					case EraseOp:
						log.Printf("[Server.Erase]%v-%v trying to erase shard %v",kv.gid,kv.me, op.Shard)
						if op.Version != kv.logNum || kv.stateMachines[op.Shard].State != Erasing{
							result.Err = ErrWrongConfig
						} else {
							kv.stateMachines[op.Shard].State = Offline
						}
					case OnlineOp:
						log.Printf("[Server.Online]%v-%v trying to onserving shard %v",kv.gid,kv.me, op.Shard)
						if op.Version != kv.logNum || kv.stateMachines[op.Shard].State != Waiting{
							result.Err = ErrWrongConfig
						} else {
							kv.stateMachines[op.Shard].State = Serving
						}
					}
					if op.Operation != UpdateOp {
						log.Printf("[Server.state]%v-%v serves %v config %v :%v",kv.gid, kv.me ,op.Shard, kv.logNum, kv.stateMachines[op.Shard])
					}
					if result.Err != ErrWrongGroup {
						kv.stateMachines[op.Shard].ClientReq[op.ClientId] = op.SeqNum
					}
				}else if op.Operation ==  GetOp {
					log.Printf("[Server.Get]%v-%v trying to duplicate get %v on shard %v",kv.gid,kv.me, op.Key, op.Shard)
					if kv.stateMachines[op.Shard].State == Serving {
						result.Value = kv.stateMachines[op.Shard].Data[op.Key]
						log.Printf("Value get on %v-%v is %v", kv.gid, kv.me, result.Value)
					} else {
						log.Printf("%v-%v Assign err.",kv.gid,kv.me)
						result.Err = ErrWrongGroup
					}
				}
				if msg.CommandIndex > kv.lastApplied {
					kv.lastApplied = msg.CommandIndex 
				}
				ch, ok := kv.waitCh[msg.CommandIndex]
				log.Printf("%v-%v sendback %v %v %v index %v result %v ok:%v",kv.gid, kv.me, op.Operation, op.Key, op.Value, msg.CommandIndex,result, ok)
				if ok {
					ch <- result 
				}
				kv.mu.Unlock()
			}
			if msg.SnapshotValid {
				log.Printf("%v-%v install snapshot",kv.gid, kv.me)
				kv.InstallSnapshot(msg.Snapshot)
				continue
			}
		}
	}
}


func (kv *ShardKV) canupdate() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Printf("[Server]%d-%d in config %v-%v is checking", kv.gid, kv.me, kv.logNum, kv.config)

	// Print all state machines' states in a single line
	stateLine := fmt.Sprintf("[Server]%d-%d Shard states: ",kv.gid, kv.me)
	for shard := 0; shard < 10; shard++ {
		stateLine += fmt.Sprintf("Shard %d: %v; ", shard, kv.stateMachines[shard].State)
	}
	log.Printf(stateLine)

	for shard := 0; shard < 10; shard++ {
		if kv.stateMachines[shard].State != Serving && kv.stateMachines[shard].State != Offline {
			return false
		}
	}
	return true
}
func (kv *ShardKV) canpull(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.stateMachines[shard].State == Pulling {
		return true
	}
	return false
}
func (kv *ShardKV) canerase(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.stateMachines[shard].State == Waiting {
		return true
	}
	return false
}

func (kv *ShardKV) controler() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		_, isleader := kv.rf.GetState()
		
		if !isleader {
			continue
		}

		if kv.canupdate() {
			config := kv.sm.Query(kv.logNum + 1)
			// log.Printf("%v-%v get config %v",kv.gid, kv.me, config)
			if config.Num == kv.logNum + 1 {
				kv.mu.Lock()
				subOp := Op{
					Operation: 		UpdateOp,
					Config: 		config,	
					Version: 		config.Num,
					PreConfig:		kv.config,
					ClientId:	 	kv.clientId + int64(130000000),
					SeqNum:			config.Num , 		
				}
				kv.mu.Unlock()
				log.Printf("[Server]%v-%v submit a update request. %v",kv.gid, kv.me,subOp)
				ok,_,_ := kv.Submit(subOp)
				log.Printf("[Server]%v-%v recieve err %v after sent update request",kv.gid, kv.me, ok)				
			}
		}
	}
}
func (kv *ShardKV) datapuller() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		_, isleader := kv.rf.GetState()
		if !isleader {
			continue
		}
		log.Printf("%v-%v is trying to pull data",kv.gid,kv.me)
		for shard := 0; shard < 10; shard ++ {
			if kv.canpull(shard) {
				gid := kv.preconfig.Shards[shard]
				log.Printf("%v-%v Go %v pull %v data",kv.gid,kv.me,gid,shard)
				if servers, ok := kv.preconfig.Groups[gid]; ok {
					log.Printf("%v-%v get server %v",kv.gid, kv.me, servers)
					for si := 0; si < len(servers); si++ {
						log.Printf("%v-%v trying pull from %v",kv.gid, kv.me, si)
						srv := kv.make_end(servers[si])
						var reply PullDataReply 
						args := PullDataArgs{
							Version: 	kv.config.Num,
							Shard: 		shard,
						}
						ok := srv.Call("ShardKV.PullData", &args, &reply)
						log.Printf("%v-%v recieve %v in datapuller from %v",kv.gid,kv.me,reply,servers[si])
						if ok && reply.Err == OK {
							kv.mu.Lock()
							subOp := Op{
								Operation: 		ActivateOp,
								Version: 		kv.config.Num,
								Data: 			reply.Data,
								Shard: 			shard,
								ClientId: 		kv.clientId + int64(shard * 100000000 + 40000000),
								SeqNum: 		kv.config.Num ,
							}
							kv.mu.Unlock()
							ok,_,_ := kv.Submit(subOp)
							if ok == OK {
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
		_, isleader := kv.rf.GetState()
		if !isleader {
			continue
		}
		for shard := 0; shard < 10; shard ++ {
			if kv.canerase(shard) {
				gid := kv.preconfig.Shards[shard]
				log.Printf("%v-%v Go %v erase %v data",kv.gid,kv.me,gid,shard)
				if servers, ok := kv.preconfig.Groups[gid]; ok {
					log.Printf("%v-%v get server %v",kv.gid, kv.me, servers)
					for si := 0; si < len(servers); si++ {
						log.Printf("%v-%v trying go erase %v",kv.gid, kv.me, si)
						srv := kv.make_end(servers[si])
						var reply EraseDataReply
						args := EraseDataArgs {
							Version: 		kv.config.Num,
							Shard: 			shard,
						}
						ok := srv.Call("ShardKV.EraseData", &args, &reply)
						log.Printf("%v-%v recieve %v in dataeraser from %v",kv.gid,kv.me,reply,servers[si])
						
						if ok && reply.Err == OK {
							kv.mu.Lock()
							subOp := Op{
								Operation: 	OnlineOp,
								Version: 	kv.config.Num,
								Shard: 		shard,
								ClientId: 	kv.clientId + int64(shard * 100000000 + 60000000),
								SeqNum: 	kv.config.Num ,	
							}
							kv.mu.Unlock()
							ok,_,_ := kv.Submit(subOp)
							if ok == OK {
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
	log.Printf("%v-%v restarted. in config %v",kv.gid,kv.me,kv.config)
	for shard := 0; shard < 10; shard ++ {
		log.Printf("%v-%v shard %v state %v",kv.gid, kv.me, shard, kv.stateMachines[shard].State)
	}
	go kv.applier()
	go kv.controler()
	go kv.datapuller()
	go kv.dataeraser()
	return kv
}
