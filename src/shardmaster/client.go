package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import (
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu sync.Mutex
	clientId 	int64
	requestId	int
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
	// Your code here.
	ck.requestId = 0
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	// Your code here.
	ck.mu.Lock()
	ck.requestId++
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(gid int, servers []string) {
	args := &JoinArgs{}
	args.GID = gid
	args.Servers = servers
	// Your code here.
	ck.mu.Lock()
	ck.requestId++
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gid int) {
	args := &LeaveArgs{}
	args.GID = gid
	// Your code here.
	ck.mu.Lock()
	ck.requestId++
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	// Your code here.
	ck.mu.Lock()
	ck.requestId++
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
