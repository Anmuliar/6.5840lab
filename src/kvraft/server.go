package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation 	OpType
	Key			string
	Value 		string
	ClientId 	int64
	SeqNum 		int
}

type OpResult struct {
	Err 		Err
	Value 		string 
	LeaderId 	int
	ClientId 	int64
	SeqNum 		int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	lastApplied 	int
	maxraftstate int // snapshot if log grows this big
	persister *raft.Persister

	// Your definitions here.
	data 		map[string]string
	clientReq   map[int64]int
	waitCh      map[int]chan OpResult
	
}
func(kv *KVServer) Snapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// snapshot := make(map[string]interface{})
	// snapshot["data"] = kv.data
	// snapshot["clientReq"] = kv.clientReq
	// snapshot["index" ] = kv.lastApplied

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.clientReq)
	e.Encode(kv.lastApplied)

	kv.rf.Snapshot(kv.lastApplied, w.Bytes())
}

func(kv *KVServer) InstallSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return 
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data 			map[string]string
	var clientReq 		map[int64]int
	var lastApplied 	int

	if d.Decode(&data) != nil ||
	   d.Decode(&clientReq) != nil ||
	   d.Decode(&lastApplied) != nil {
		// log.Printf("Failed to decode the snapshot")
	} else {
		kv.data = data
		kv.clientReq = clientReq
		kv.lastApplied = lastApplied
	}
	DPrintf("server %v recovered statemachine snapshot %v %v %v", kv.me, kv.data, kv.clientReq, kv.lastApplied)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Operation: 	GetOp,
		Key:		args.Key,
		ClientId: 	args.ClientId,
		SeqNum: 	args.SeqNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.LeaderId = -1
		return 
	}

	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.waitCh[index] = ch 
	kv.mu.Unlock()
	select {
	case committedOp := <-ch:
		if committedOp.ClientId == args.ClientId && committedOp.SeqNum == op.SeqNum {
			kv.mu.Lock()
			reply.Value = kv.data[op.Key]
			reply.Err =committedOp.Err
			reply.LeaderId = committedOp.LeaderId
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
			reply.LeaderId = kv.rf.GetLeader()
		}
	case <-time.After(500 * time.Millisecond) :
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.waitCh, index) 
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation:  args.Op,
		Key:		args.Key,
		Value: 		args.Value,
		ClientId:	args.ClientId,
		SeqNum:		args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader 
		reply.LeaderId = -1
		// log.Printf("return!")
		return 
	}

	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.waitCh[index] = ch
	kv.mu.Unlock()
	select {
	case committedOp := <- ch:
		if committedOp.SeqNum == args.SeqNum {
			reply.Err = committedOp.Err
			reply.LeaderId = committedOp.LeaderId
		} else {
			reply.Err = ErrWrongLeader 
			reply.LeaderId = kv.rf.GetLeader()
		}
	case <-time.After(500 * time.Millisecond) :
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.waitCh, index)
	kv.mu.Unlock()

}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			// log.Printf("%v recieve msg %v from raft", kv.me, msg)
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.Snapshot()
			}
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				result := OpResult {
					Err: 		OK,
					SeqNum: 	op.SeqNum,
					ClientId:	op.ClientId,
				}
				lastSeq, flag := kv.clientReq[op.ClientId]
				if !flag || op.SeqNum > lastSeq {
					switch op.Operation {
					case PutOp:
						kv.data[op.Key] = op.Value
					case AppendOp:
						kv.data[op.Key] += op.Value
					case GetOp:
						result.Value = kv.data[op.Key]
					}
					kv.clientReq[op.ClientId] = op.SeqNum
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	// log.Printf("server %v created.",kv.me)
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.clientReq = make(map[int64]int)
	kv.waitCh = make(map[int]chan OpResult)


	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.InstallSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.applier()
	return kv
}
