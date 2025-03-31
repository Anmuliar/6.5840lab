package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "log"

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	clientId 	int64
	seqNum		int
	leaderId 	int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.seqNum = 0 
	ck.leaderId = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqNum++
	args := GetArgs{
		Key: 		key,
		ClientId:	ck.clientId,
		SeqNum:		ck.seqNum,
	}
	for  {
		reply := GetReply{}
		log.Printf("client %v request to Get to server %v\n",ck.clientId, ck.leaderId)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			if ok && reply.LeaderId != -1 {
				ck.leaderId = reply.LeaderId
				continue
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == OK {
			log.Printf("Value on key %v is %v", key, reply.Value)
			return reply.Value
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	// You will have to modify this function.
	ck.seqNum++
	args := PutAppendArgs {
		Key:		key,
		Value:		value,
		Op:			op,
		ClientId:	ck.clientId,
		SeqNum:		ck.seqNum,
	}
	
	for {
		reply := PutAppendReply{}

		log.Printf("client %v request to Put/Append to server %v, args:%v\n",ck.clientId, ck.leaderId, args)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		log.Printf("%v %v %v",ok, reply, args)
		if !ok || reply.Err == ErrWrongLeader {
			if ok && reply.LeaderId != -1 {
				ck.leaderId = reply.LeaderId 
				continue
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == OK {
			return 
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
