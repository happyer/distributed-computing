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

import (
	//"fmt"
	"sync"
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
)

const (
	LEADER = iota
	CANDIDATE
	FLLOWER

	HBINTERVAL = 50 * time.Millisecond // 50ms
)


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

type LogEntry struct {
	LogIndex   int
	LogTerm    int
	LogCommand interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex
	peers         []*labrpc.ClientEnd
	persister     *Persister
	me            int // index into peers[]

			  // Your data here.
			  // Look at the paper's Figure 2 for a description of what
			  // state a Raft server must maintain.

			  //channel
	state         int
	voteCount     int
	chanCommit    chan bool
	chanHeartbeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanApply     chan ApplyMsg

			  //persistent state on all server
	currentTerm   int
	votedFor      int
	log           []LogEntry

			  //volatile state on all servers
	commitIndex   int
	lastApplied   int

			  //volatile state on leader
	nextIndex     []int
	matchIndex    []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log) - 1].LogIndex
}
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log) - 1].LogTerm
}
func (rf *Raft) IsLeader() bool {
	return rf.state == LEADER
}
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readSnapshot(data []byte) {

	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex

	rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}

	go func() {
		rf.chanApply <- msg
	}()
}
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here.
	Term      int
	Success   bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()
	uptoDate := false



	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	//Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms,then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date
	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index {
		// at least up to date
		uptoDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptoDate {
		rf.chanGrantVote <- true
		rf.state = FLLOWER
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	//Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	rf.chanHeartbeat <- true
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	baseIndex := rf.log[0].LogIndex


	// If a follower’s log is inconsistent with the leader’s, the AppendEntries consis- tency check will fail in the next AppendEntries RPC.
	// Af- ter a rejection, the leader decrements nextIndex and retries the AppendEntries RPC
	//Eventually nextIndex will reach a point where the leader and follower logs match
	//which removes any conflicting entries in the follower’s log and appends entries from the leader’s log (if any).
	if args.PrevLogIndex > baseIndex {
		term := rf.log[args.PrevLogIndex - baseIndex].LogTerm
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.log[i - baseIndex].LogTerm != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}
	if args.PrevLogIndex < baseIndex {

	} else {
		//Append any new entries not already in the log
		rf.log = rf.log[: args.PrevLogIndex + 1 - baseIndex]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}
	//If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}
	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		term := rf.currentTerm
		if rf.state != CANDIDATE {
			return ok
		}
		if args.Term != term {
			return ok
		}
		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = FLLOWER
			rf.votedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == CANDIDATE && rf.voteCount > len(rf.peers) / 2 {
				rf.state = FLLOWER
				rf.chanLeader <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].LogIndex + 1
				//reply.NextIndex
				//rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) GetPerisistSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []LogEntry

	newLogEntries = append(newLogEntries, LogEntry{LogIndex: index, LogTerm: rf.log[index - baseIndex].LogTerm})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i - baseIndex])
	}

	rf.log = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].LogIndex)
	e.Encode(newLogEntries[0].LogTerm)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {

	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{LogIndex: lastIncludedIndex, LogTerm: lastIncludedTerm})

	for index := len(log) - 1; index >= 0; index-- {
		if log[index].LogIndex == lastIncludedIndex && log[index].LogTerm == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[index + 1:]...)
			break
		}
	}

	return newLogEntries
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.chanHeartbeat <- true
	rf.state = FLLOWER
	rf.currentTerm = rf.currentTerm

	rf.persister.SaveSnapshot(args.Data)

	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()

	rf.chanApply <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FLLOWER
			rf.votedFor = -1
			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.getLastIndex() + 1
		//fmt.Printf("raft:%d start\n",rf.me)
		rf.log = append(rf.log, LogEntry{LogTerm:term, LogCommand:command, LogIndex:index}) // append new entry from client
		rf.persist()
	}
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

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}

/**
 * Log replication
 */
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	N := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex
	//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i - baseIndex].LogTerm == rf.currentTerm {
				num++
			}
		}
		if 2 * num > len(rf.peers) {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {

			//copy(args.Entries, rf.log[args.PrevLogIndex + 1:])

			if rf.nextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				//	fmt.Printf("baseIndex:%d PrevLogIndex:%d\n",baseIndex,args.PrevLogIndex )
				args.PrevLogTerm = rf.log[args.PrevLogIndex - baseIndex].LogTerm
				//args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex + 1:]))
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex + 1 - baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex + 1 - baseIndex:])
				args.LeaderCommit = rf.commitIndex
				go func(i int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(i, args, &reply)
				}(i, args)
			} else {
				var args InstallSnapshotArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.log[0].LogIndex
				args.LastIncludedTerm = rf.log[0].LogTerm
				args.Data = rf.persister.snapshot
				go func(server int, args InstallSnapshotArgs) {
					reply := &InstallSnapshotReply{}
					rf.sendInstallSnapshot(server, args, reply)
				}(i, args)
			}
		}
	}
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
	rf.state = FLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanApply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go func() {
		for {
			switch rf.state {
			case FLLOWER:
					select {
					case <-rf.chanHeartbeat:
					case <-rf.chanGrantVote:
					case <-time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
						rf.state = CANDIDATE
					}
			case LEADER:
				//fmt.Printf("Leader:%v %v\n",rf.me,"boatcastAppendEntries	")
				rf.broadcastAppendEntries()
				time.Sleep(HBINTERVAL)
			case CANDIDATE:
				rf.mu.Lock()
				//To begin an election, a follower increments its current term and transitions to candidate state
				rf.currentTerm++
				//It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				//(a) it wins the election, (b) another server establishes itself as leader, or (c) a period of time goes by with no winner
				go rf.broadcastRequestVote()
					select {
					case <-time.After(time.Duration(rand.Int63() % 300 + 510) * time.Millisecond):
					case <-rf.chanHeartbeat:
						rf.state = FLLOWER
					//	fmt.Printf("CANDIDATE %v reveive chanHeartbeat\n",rf.me)
					case <-rf.chanLeader:
						rf.mu.Lock()
						rf.state = LEADER
					//fmt.Printf("%v is Leader\n",rf.me)
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.peers {
							//The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower.
							// When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
							rf.nextIndex[i] = rf.getLastIndex() + 1
							rf.matchIndex[i] = 0
						}
						rf.mu.Unlock()
					//rf.boatcastAppendEntries()
					}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.log[0].LogIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i - baseIndex].LogCommand}
					applyCh <- msg
					//fmt.Printf("me:%d %v\n",rf.me,msg)
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
