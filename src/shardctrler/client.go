package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	clientId	int64
	seqNum 		int
	// Your data here.
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
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seqNum++
	args := &QueryArgs{
		Num:		num,
		ClientId:	ck.clientId,
		SeqNum:		ck.seqNum,
	}
	// Your code here.
	// args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqNum++
	args := &JoinArgs{
		Servers:		servers,
		ClientId:		ck.clientId,
		SeqNum:			ck.seqNum,
	}
	// Your code here.
	// args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqNum++
	args := &LeaveArgs{
		GIDs:		gids,
		ClientId:	ck.clientId,
		SeqNum:		ck.seqNum,
	}
	// Your code here.
	// args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqNum++
	args := &MoveArgs{
		Shard:		shard,
		GID:		gid,
		ClientId:	ck.clientId,
		SeqNum:		ck.seqNum,
	}
	// Your code here.
	// args.Shard = shard
	// args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
