package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady	  = "ErrNotReady"
	ErrWrongConfig= "ErrWrongCOnfig"
)

const (
	Get = "Get"
	Put = "Put"
	Append = "Append"
	PutAppend = "PutAppend"
	Reconfigure	= "Configure"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId	int64
	RequestId	int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId	int64
	RequestId	int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

/*--------------Add by Yang----------------------*/
// send to follower server in group
type ReconfigureArgs struct {
	Cfg			shardmaster.Config
	StoreShard 	[shardmaster.NShards]map[string]string
	Ack 		map[int64]int
	//Replies		map[int64]Result //!!! be careful of gob
}

type ReconfigureReply struct {
	Err			Err
}

// send to another group leader
type TransferArgs struct {
	ConfigNum	int
	Shards		[]int
}

 type TransferReply struct {
	 StoreShard 	[shardmaster.NShards]map[string]string
	 Ack 			map[int64]int
	 WrongLeader	bool
	 Err 			Err
 }