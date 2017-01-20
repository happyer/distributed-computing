package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"os"
	"runtime/debug"
)

// import "bytes"
// import "encoding/gob"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//


type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

type Role int

const (
	Leader Role = 1 + iota
	Candidate
	Follower
)

const RaftElectionTimeoutLow = 150 * time.Microsecond
const RaftElectionTimeoutHigh = 300 * time.Microsecond
const RaftHeartbeatPeriod = 100 * time.Microsecond

type Raft struct {
	mu            sync.Mutex
	peers         []*labrpc.ClientEnd
	persister     *Persister
	me            int // index into peers[]

			  // Your data here.
			  // Look at the paper's Figure 2 for a description of what
			  // state a Raft server must maintain.
	currtentTerm  int
	voteFor       int
	logs          []LogEntry


			  //volatile state ont all server
	commitIndex   int
	lastApplied   int
			  //volatile state on leader
	nextIndex     []int
	matchIndex    []int

			  //append entries rpc
	leaderId      int
	role          Role

			  //for communication
	chanRole      chan Role
	chanCommitted chan ApplyMsg

			  //timer
	chanHeartbeat chan bool
	chanGrantVote chan bool

			  //for leader to control follower
	chanAppend    []chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currtentTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currtentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currtentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.logs)

}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppenEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppenEntriesReply struct {
	Term    int
	Success bool
	//option
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//persist  for
	defer rf.persist()
	reply = new(RequestVoteReply)

	//if term <currentTerm ,reply false
	if args.Term < rf.currtentTerm {
		reply.Term = rf.currtentTerm
		reply.VoteGranted = false
		return
	}


	//IF RPC Request or response contains term T > currentTerm set currentTerm = T,convert to follower
	if rf.currtentTerm < args.Term {
		rf.currtentTerm = args.Term
		rf.voteFor = -1
		rf.role = Follower
		rf.chanRole <- Follower
	}


	reply.Term = rf.currtentTerm
	/**
	 *   If votedFor is null or candidateId, and candidate’s log is at
         *     least as up-to-date as receiver’s log, grant vote
	 *   Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	 *   If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	 *   If the logs end with the same term, then whichever log is longer is more up-to-date.
	 */
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
	}else if rf.logs[len(rf.logs) - 1].Term > args.LastLogTerm {
		reply.VoteGranted = false
	}else if rf.logs[len(rf.logs)-1].Term == args.Term && rf.logs[len(rf.logs)-1].Index > args.LastLogIndex  {
		reply.VoteGranted = false
	}else {
		rf.chanGrantVote <- true
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//






func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



/**
To bring a follower’s log into consistency with its own, the leader must find the latest log entry where the two logs agree,
 delete any entries in the follower’s log after that point, and send the follower all of the leader’s entries after that point.
 */
func (rf * Raft) AppendEntries(args AppenEntriesArgs,reply *AppenEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currtentTerm {
		reply.Term = rf.currtentTerm
		reply.Success = false
		return
	}
	rf.chanHeartbeat <- true

	//IF RPC Request or response contains term T > currentTerm set currentTerm = T,convert to follower
	if args.Term > rf.currtentTerm {
		rf.currtentTerm = args.Term
		rf.voteFor = -1
		rf.leaderId = args.LeaderId
		rf.role = Follower
		rf.chanRole <- Follower
	}

	reply.Term = args.Term

	//from same inde and same term and entry is same rule
	baseLogIndex := rf.logs[0].Index
	if baseLogIndex  > args.PrevLogIndex {
		reply.Success = false
		//option
		reply.NextIndex = baseLogIndex +1
	}



	/***
		The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower
	 */

	//When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
	//Af- ter a rejection, the leader decrements nextIndex and retries the AppendEntries RPC
	//Eventually nextIndex will reach a point where the leader and follower logs match
	//When this happens, AppendEntries will succeed, which removes any conflicting entries in the follower’s log and appends entries from the leader’s log (if any)
	//Once AppendEntries succeeds, the follower’s log is consistent with the leader’s, and it will remain that way for the rest of the term
	//如果跟随者在它的日志中找不到包含相同索引位置和任期号的条目，那么他就会拒绝接收新的日志条目
	if baseLogIndex + len(rf.logs) <= args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = baseLogIndex + len(rf.logs)
	}


	// optimized for backing quickly
	// include the term of the conflicting entry and the first index it stores for that term.
	if rf.logs[args.PrevLogIndex - baseLogIndex].Term != args.PreLogTerm {
		reply.Success = false
		nextIndex := args.PrevLogIndex
		failTerm := rf.logs[nextIndex - baseLogIndex].Term
		//! the loop must execute one time when nextIndex > 0
		for nextIndex >= baseLogIndex && rf.logs[nextIndex-baseLogIndex].Term == failTerm {
			nextIndex--
		}
		// reply.NextIndex == baseLogIndex when need snapshot
		reply.NextIndex = nextIndex + 1
		//DPrintf("unmatch in server [%d]: AppendEntries: backing to [%d]",
		//	rf.me, reply.NextIndex)
		return
	}

	var updateLogIndex int
	if len(args.Entries) != 0 {
		//!!! erase those conflicting with a new one all that following
		DPrintf(rf.me, "receive append:", args)
		for i, entry := range args.Entries {
			curIndex := entry.Index - baseLogIndex
			if curIndex < len(rf.logs) {
				if curIndex < baseLogIndex || curIndex > rf.logs[len(rf.logs)-1].Index {
					//DPrintf("logTable: from [%d] to [%d] while PreLogIndex [%d] and entry index [%d]",
					//	baseLogIndex, rf.logTable[len(rf.logTable) - 1].Index, args.PrevLogIndex, entry.Index)
				}
				if entry.Term != rf.logs[curIndex].Term {
					//DPrintf("[%d] in term [%d] replace entry [%d] [%d]",
					//	rf.me, rf.currentTerm, curIndex, rf.logTable[curIndex].Command)
					rf.logs = append(rf.logs[:curIndex], entry)
				}
			} else {
				//DPrintf("[%d] in term [%d] append entry [%d] [%d]",
				//	rf.me, rf.currentTerm, curIndex, rf.logTable[curIndex].Command)
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			}
		}
		reply.NextIndex = rf.logs[len(rf.logs)-1].Index + 1
		updateLogIndex = reply.NextIndex - 1
	} else {
		reply.NextIndex = args.PrevLogIndex + 1
		updateLogIndex = args.PrevLogIndex
	}
	reply.Success = true
	//!!! the last new entry is update log index
	rf.updateFollowCommit(args.LeaderCommit, updateLogIndex)

}




func (rf *Raft) doAppendEntries(server int) {
	//! make a lock for every follower to serialize
	select {
	case <- rf.chanAppend[server]:
	default: return
	}
	defer func() {rf.chanAppend[server] <- true}()

	for rf.role == Leader {
		rf.mu.Lock()
		baseLogIndex := rf.logs[0].Index
		//DPrintf("[%d] append to [%d]: base [%d], length [%d] and next [%d]",
		//	rf.me, server, baseLogIndex, len(rf.logs), rf.nextIndex[server])
		if rf.nextIndex[server] <= baseLogIndex {
			var args InstallSnapshotArgs
			args.Term = rf.currtentTerm
			args.LeaderId = rf.me
			args.LastIncludedIndex = rf.logs[0].Index
			args.LastIncludedTerm = rf.logs[0].Term
			args.Data = rf.persister.snapshot
			rf.mu.Unlock()
			var reply InstallSnapshotReply
			if rf.sendInstallSnapshot(server, args, &reply) {
				if rf.role != Leader {
					return
				}
				if args.Term != rf.currtentTerm || reply.Term > args.Term {
					if reply.Term > args.Term {
						rf.mu.Lock()
						rf.currtentTerm = reply.Term
						rf.voteFor = -1
						rf.role = Follower
						rf.chanRole <- Follower
						rf.persist()
						rf.mu.Unlock()
					}
					return
				}
				rf.matchIndex[server] = baseLogIndex
				rf.nextIndex[server] = baseLogIndex + 1
				return
			}
		} else {
			// Basic argument
			var args AppenEntriesArgs
			args.Term = rf.currtentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex - baseLogIndex < 0 || args.PrevLogIndex - baseLogIndex >= len(rf.logs) {
				DPrintf("base:", baseLogIndex, "largest:", rf.logs[len(rf.logs)-1].Index,
					"PrevLogIndex:", args.PrevLogIndex, "nextIndex:", rf.nextIndex[server])
				debug.PrintStack()
				os.Exit(-1)
			}
			args.PreLogTerm = rf.logs[args.PrevLogIndex - baseLogIndex].Term
			args.LeaderCommit = rf.commitIndex
			// Entries
			if rf.nextIndex[server] < baseLogIndex + len(rf.logs) {
				//if again {
				//	again = false
				//	DPrintf("[%d] in term [%d] send entry to [%d]: [%d] [%d] to [%d] [%d]",
				//		rf.me, rf.currentTerm,
				//		server, rf.nextIndex[server], rf.logs[rf.nextIndex[server]].Command,
				//		len(rf.logs) - 1, rf.logs[len(rf.logs) - 1].Command)
				//}
				args.Entries = rf.logs[rf.nextIndex[server] - baseLogIndex:]
			}
			rf.mu.Unlock()

			// Reply
			var reply AppenEntriesReply
			if rf.sendAppendEntries(server, args, &reply) {
				rf.DoAppendEntriesReply(server, args, reply)
				return
			} else {
				//DPrintf("doAppendEntries: no reply from follower [%d]", server)
			}
		}
	}
}

func (rf *Raft) DoAppendEntriesReply(server int, args AppenEntriesArgs, reply AppenEntriesReply) {
	//!!!after a long time: in case of role changing while appending
	if rf.role != Leader {
		return
	}
	if args.Term != rf.currtentTerm || reply.Term > args.Term {
		if reply.Term > args.Term {
			rf.mu.Lock()
			rf.currtentTerm = reply.Term
			rf.voteFor = -1
			rf.role = Follower
			rf.chanRole <- Follower
			rf.persist()
			rf.mu.Unlock()
		}
		return
	}
	//!!!after a long time: heartbeat RPC reply fail index as well
	if reply.Success {
		rf.matchIndex[server] = reply.NextIndex - 1
		rf.nextIndex[server] = reply.NextIndex
	} else {
		//! out of range
		////rf.matchIndex[server] = 0
		//if reply.NextIndex > rf.matchIndex[server] + 1 {
		//	rf.nextIndex[server] = reply.NextIndex
		//} else {
		//	rf.nextIndex[server] = rf.matchIndex[server] + 1
		//}
		rf.nextIndex[server] = reply.NextIndex
	}
}


func (rf *Raft) sendAppendEntries(server int, args AppenEntriesArgs, reply *AppenEntriesReply) bool {
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		c <- ok
	}()
	select {
	case ret = <- c:
	case <-time.After(RaftHeartbeatPeriod):
		ret = false
	}
	return ret
}



//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}


func (rf *Raft) updateFollowCommit(leaderCommit int, lastIndex int) {
	oldVal := rf.commitIndex
	if leaderCommit > rf.commitIndex {
		if leaderCommit < lastIndex {
			rf.commitIndex = leaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}
	//! update the log added in previous term
	baseIndex := rf.logs[0].Index
	for oldVal++; oldVal <= rf.commitIndex; oldVal++ {
		//DPrintf("[%d] follower apply: [%d] [%d]", rf.me, oldVal, rf.logTable[oldVal-baseIndex].Command)
		rf.chanCommitted <- ApplyMsg{Index:oldVal, Command:rf.logs[oldVal-baseIndex].Command}
		rf.lastApplied = oldVal
	}
}



/* -----------------lab3----------*/
type InstallSnapshotArgs struct {
	Term int	// leader’s term
	LeaderId int	//so follower can redirect clients
	LastIncludedIndex int	// the snapshot replaces all entries
	LastIncludedTerm int // term of lastIncludedIndex
	Data []byte // raw bytes of the snapshot chunk, starting at offset
	//Offset int	// byte offset where chunk is positioned
	//Done bool	// true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int	// currentTerm, for leader to update itself
}


func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	var LastIncludedIndex int
	var LastIncludedTerm int
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	rf.logs = rf.truncateLog(LastIncludedIndex, LastIncludedTerm)
	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	rf.chanCommitted <- msg
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currtentTerm {
		reply.Term = rf.currtentTerm
		return
	}
	rf.chanHeartbeat <- true

	if rf.currtentTerm < args.Term {
		rf.currtentTerm = args.Term
		rf.voteFor = -1
		rf.role = Follower
		rf.chanRole <- Follower
	}
	reply.Term = args.Term

	baseIndex := rf.logs[0].Index
	if args.LastIncludedIndex < baseIndex {
		return
	}

	rf.logs = rf.truncateLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persister.SaveSnapshot(args.Data)
	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.chanCommitted <- msg
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		c <- ok
	}()
	select {
	case ret = <- c:
	case <-time.After(RaftHeartbeatPeriod):
		ret = false
	}
	return ret
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.logs[0].Index || index > rf.logs[len(rf.logs)-1].Index {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	rf.logs = rf.logs[index-rf.logs[0].Index:]
	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.logs[0].Index)
	e.Encode(rf.logs[0].Term)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func (rf *Raft) truncateLog(index int, term int) []LogEntry {
	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: term})
	//!!! Be careful of variable name overwrite
	for  i:= len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Index == index && rf.logs[i].Term == term {
			newLogEntries = append(newLogEntries, rf.logs[i+1:]...)
			break
		}
	}
	return newLogEntries
}




/**--------lab3---------*/