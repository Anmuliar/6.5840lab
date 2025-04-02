package shardctrler
import "log"
//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config *Config) ShardBalance() {
	GroupNum := len(config.Groups)
	log.Printf("before:%v",config)
	if GroupNum == 0 {
		return 
	}
	shardsize := int(10 / GroupNum)
	extrashard :=  10 % GroupNum
	for gid, _ := range config.Groups {
		ShardAssigned := 0
		for shard := range config.Shards {
			if config.Shards[shard] == gid {
				if extrashard > 0 {
					if ShardAssigned >= shardsize + 1{
						config.Shards[shard] = 0
					} else {
						ShardAssigned ++
					}
				} else {
					if ShardAssigned >= shardsize {
						config.Shards[shard] = 0
					} else {
						ShardAssigned ++
					}
				}
			}
		}
		if ShardAssigned == shardsize + 1{
			extrashard --
		}
	}
	log.Printf("remove:%v",config)
	for gid, _ := range config.Groups {
		ShardAssigned := 0
		for shard := range config.Shards {
			if config.Shards[shard] == gid {
				ShardAssigned ++
			}
		}
		if ShardAssigned == shardsize + 1{
			continue
		} 
		for shard := range config.Shards {
			if config.Shards[shard] == 0 {
				if extrashard > 0 {
					if ShardAssigned < shardsize + 1 {
						ShardAssigned ++
						config.Shards[shard]= gid
					}
				} else {
					if ShardAssigned < shardsize {
						ShardAssigned ++
						config.Shards[shard] = gid
					}
				}
			}
		}
		if ShardAssigned == shardsize + 1{
			extrashard --
		}
	}
	log.Printf("final:%v",config)
}
type OpType int 
const (
	JoinOp 		OpType = 0
	LeaveOp		OpType = 1
	MoveOp		OpType = 2
	QueryOp		OpType = 3
)
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout 	   = "ErrTimeout"
)


type Err string

type JoinArgs struct {
	Servers 	map[int][]string // new GID -> servers mappings
	ClientId 	int64
	SeqNum		int
}

type JoinReply struct {
	Err         Err
	LeaderId 	int
}

type LeaveArgs struct {
	GIDs 		[]int
	ClientId	int64
	SeqNum		int
}

type LeaveReply struct {
	Err         Err
	LeaderId 	int
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientId 	int64
	SeqNum		int
}

type MoveReply struct {
	Err         Err
	LeaderId 	int
}

type QueryArgs struct {
	Num int // desired config number
	ClientId 	int64
	SeqNum		int
}

type QueryReply struct {
	Err         Err
	LeaderId 	int
	Config      Config
}
