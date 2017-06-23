package shardmaster2


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"time"
	"runtime/debug"
	"os"
	"log"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	cfgNum	int
	ack		map[int64]int
	messages map[int]chan Result

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	OpType	string
	Args	interface{}
}





func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	index, _, isLeader := sm.rf.Start(Op{OpType:Join,Args:args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	if _,ok:=sm.messages[index];!ok{
		sm.messages[index] = make(chan Result,1)
	}
	msg := sm.messages[index]
	sm.mu.Unlock()


	select {
	case msg := <- msg:
		if recArgs, ok := msg.args.(JoinArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recArgs.ClientId || args.RequestId != recArgs.RequestId {
				reply.WrongLeader = true
			} else {
				reply.Err = msg.reply.(JoinReply).Err
				reply.WrongLeader = false
				//DPrintf("[%d] Apply Join: [%d]", sm.me, args.RequestId)
			}
		}
	case <- time.After(time.Second * 1):
		reply.WrongLeader = true
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	index, _, isLeader := sm.rf.Start(Op{OpType: Leave, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	if _, ok := sm.messages[index]; !ok {
		sm.messages[index] = make(chan Result, 1)

	}
	msg := sm.messages[index]
	sm.mu.Unlock()

	select {
	case msg := <- msg:
		if recArgs, ok := msg.args.(LeaveArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recArgs.ClientId || args.RequestId != recArgs.RequestId {
				reply.WrongLeader = true
			} else {
				reply.Err = msg.reply.(LeaveReply).Err
				reply.WrongLeader = false
				//DPrintf("[%d] Apply Leave: [%d]", sm.me, args.RequestId)
			}
		}
	case <- time.After(time.Second * 1):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	index, _, isLeader := sm.rf.Start(Op{OpType: Move, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	if _, ok := sm.messages[index]; !ok {
		sm.messages[index] = make(chan Result, 1)

	}
	chanMsg := sm.messages[index]
	sm.mu.Unlock()

	select {
	case msg := <- chanMsg:
		if recArgs, ok := msg.args.(MoveArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recArgs.ClientId || args.RequestId != recArgs.RequestId {
				reply.WrongLeader = true
			} else {
				reply.Err = msg.reply.(MoveReply).Err
				reply.WrongLeader = false
				//DPrintf("[%d] Apply Move: [%d]", sm.me, args.RequestId)
			}
		}
	case <- time.After(time.Second * 1):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	index, _, isLeader := sm.rf.Start(Op{OpType: Query, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	if _, ok := sm.messages[index]; !ok {
		sm.messages[index] = make(chan Result, 1)

	}
	chanMsg := sm.messages[index]
	sm.mu.Unlock()

	select {
	case msg := <- chanMsg:
		if recArgs, ok := msg.args.(QueryArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recArgs.ClientId || args.RequestId != recArgs.RequestId {
				reply.WrongLeader = true
			} else {
				//!!! return query value
				*reply = msg.reply.(QueryReply)
				reply.WrongLeader = false
				//DPrintf("[%d] Apply Query: [%d]", sm.me, args.RequestId)
			}
		}
	case <- time.After(time.Second * 1):
		reply.WrongLeader = true
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)

	// Your code here.
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveReply{})
	gob.Register(MoveReply{})
	gob.Register(QueryReply{})
	sm.cfgNum = 0
	sm.ack = make(map[int64]int)
	sm.messages = make(map[int]chan Result, 1)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.Update()

	return sm
}



func (sm *ShardMaster) ReBalanceShards(cfg *Config, request string, gid int) {
	shardsCount := sm.CountShards(cfg) // gid -> number of shards
	switch request {
	case Join:
		meanNum := NShards / len(cfg.Groups)
		for i := 0; i < meanNum; i++ {
			maxGid := sm.GetMaxGidByShards(shardsCount)
			if len(shardsCount[maxGid]) == 0 {
				DPrintf("ReBalanceShards: max gid does not have shards")
				debug.PrintStack()
				os.Exit(-1)
			}
			cfg.Shards[shardsCount[maxGid][0]] = gid
			shardsCount[maxGid] = shardsCount[maxGid][1:]
		}
	case Leave:
		shardsArray := shardsCount[gid]
		delete(shardsCount, gid)
		for _, v := range(shardsArray) {
			minGid := sm.GetMinGidByShards(shardsCount)
			cfg.Shards[v] = minGid
			shardsCount[minGid] = append(shardsCount[minGid], v)
		}
	}
}

func (sm *ShardMaster) GetMaxGidByShards(shardsCount map[int][]int) int {
	max := -1
	var gid int
	for k, v := range shardsCount {
		if max < len(v) {
			max = len(v)
			gid = k
		}
	}
	return gid
}

func (sm *ShardMaster) GetMinGidByShards(shardsCount map[int][]int) int {
	min := -1
	var gid int
	for k, v := range shardsCount {
		if min == -1 || min > len(v) {
			min = len(v)
			gid = k
		}
	}
	return gid
}

func (sm *ShardMaster) CountShards(cfg *Config) map[int][]int {
	shardsCount := map[int][]int{}
	for k := range cfg.Groups {
		shardsCount[k] = []int{}
	}
	for k, v := range cfg.Shards {
		shardsCount[v] = append(shardsCount[v], k)
	}
	return shardsCount
}

// receive command form raft to update database
func (sm *ShardMaster) Update() {
	for true {
		msg := <- sm.applyCh
		request := msg.Command.(Op)
		//!!! value and type is a type of variable
		var result Result
		var clientId int64
		var requestId int
		switch request.OpType {
		case Join:
			args := request.Args.(JoinArgs)
			clientId = args.ClientId
			requestId = args.RequestId
			result.args = args
		case Leave:
			args := request.Args.(LeaveArgs)
			clientId = args.ClientId
			requestId = args.RequestId
			result.args = args
		case Move:
			args := request.Args.(MoveArgs)
			clientId = args.ClientId
			requestId = args.RequestId
			result.args = args
		case Query:
			args := request.Args.(QueryArgs)
			clientId = args.ClientId
			requestId = args.RequestId
			result.args = args
		}

		result.opType = request.OpType
		result.reply = sm.Apply(request, sm.IsDuplicated(clientId, requestId))
		sm.SendResult(msg.Index, result)
		sm.CheckValid()
	}
}

func (sm *ShardMaster) Apply(request Op, isDuplicated bool) interface{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	switch request.Args.(type) {
	case JoinArgs:
		var reply JoinReply
		if !isDuplicated {
			sm.ApplyJoin(request.Args.(JoinArgs))
			DPrintln(sm.me, "apply Join",  request.Args.(JoinArgs), "->", sm.configs[sm.cfgNum])
		}
		reply.Err = OK
		return reply
	case LeaveArgs:
		var reply LeaveReply
		if !isDuplicated {
			sm.ApplyLeave(request.Args.(LeaveArgs))
			DPrintln(sm.me, "apply Leave",  request.Args.(LeaveArgs), "->", sm.configs[sm.cfgNum])
		}
		reply.Err = OK
		return reply
	case MoveArgs:
		var reply MoveReply
		if !isDuplicated {
			sm.ApplyMove(request.Args.(MoveArgs))
		}
		reply.Err = OK
		DPrintln(sm.me, "apply Move",  request.Args.(MoveArgs), "->", sm.configs[sm.cfgNum])
		return reply
	case QueryArgs:
		var reply QueryReply
		args := request.Args.(QueryArgs)
		if args.Num == -1 || args.Num > sm.cfgNum {
			reply.Config = sm.configs[sm.cfgNum]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		reply.Err = OK
		DPrintln(sm.me, "apply Query",  request.Args.(QueryArgs), "->", sm.configs[sm.cfgNum])
		return reply
	}
	return nil
}

func (sm *ShardMaster) IsDuplicated(clientId int64, requestId int) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if value, ok := sm.ack[clientId]; ok && value >= requestId {
		return true
	}
	sm.ack[clientId] = requestId
	return false
}

func (sm *ShardMaster) SendResult(index int, result Result) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.messages[index]; !ok {
		sm.messages[index] = make(chan Result, 1)
	} else {
		select {
		case <- sm.messages[index]:
		default:
		}
	}
	sm.messages[index] <- result
}

//!!! Be careful of what the variable means
func (sm *ShardMaster) CheckValid() {
	c := sm.configs[sm.cfgNum]
	for _, v := range c.Shards {
		//!!! init group is zero
		if len(c.Groups) == 0 && v == 0 {
			continue
		}
		if _, ok := c.Groups[v]; !ok {
			DPrintln("Check failed that", v, "group does not exit", c.Shards, c.Groups)
			debug.PrintStack()
			os.Exit(-1)
		}
	}
}


func (sm *ShardMaster) NextConfig() *Config {
	var c Config
	c.Num = sm.cfgNum + 1
	c.Shards = sm.configs[sm.cfgNum].Shards
	c.Groups = map[int][]string{}
	for k, v := range sm.configs[sm.cfgNum].Groups {
		c.Groups[k] = v
	}
	sm.cfgNum += 1
	sm.configs = append(sm.configs, c)
	//!!! return reference
	return &sm.configs[sm.cfgNum]
}

func (sm *ShardMaster) ApplyJoin(args JoinArgs) {
	cfg := sm.NextConfig()
	//!!! consider whether gid is exist or not
	if _, exist := cfg.Groups[args.GID]; !exist {
		//cfg.Groups[args.GID] = args.Servers
		cfg.Groups = args.Servers
		sm.ReBalanceShards(cfg, Join, args.GID)
	}
}

func (sm *ShardMaster) ApplyLeave(args LeaveArgs) {
	cfg := sm.NextConfig()
	//!!! consider whether gid is exist or not
	if _, exist := cfg.Groups[args.GID]; exist {
		delete(cfg.Groups, args.GID)
		sm.ReBalanceShards(cfg, Leave, args.GID)
	}
}

func (sm *ShardMaster) ApplyMove(args MoveArgs) {
	cfg := sm.NextConfig()
	cfg.Shards[args.Shard] = args.GID
}

const Debug = 0
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) {
	if Debug > 0 {
		log.Println(a...)
	}
	return
}
