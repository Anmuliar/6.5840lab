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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	// "fmt"
	// "./util"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type applySignal struct {}

type Entry struct {
	Command 	 interface{}
	Term 		 int
	Index		 int
}
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	elect	  bool 				  // should enter election phase or not
	applyCh   chan ApplyMsg
	applySignalCh chan applySignal
	randomElectionTimeout int64
	applierCond 	*sync.Cond
	applierActive 	bool


	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm 	int           // Latest term has seen, begins from 0
	votedFor 		int 		  // Which leader voted for
	log				[]Entry		  // log stored
	isleader 		bool 
	lastIncludedIndex 		int
	lastIncludedTerm 		int
	snapshot 		[]byte


	// Volatile state on all servers

	commitIndex		int			  // Highest index of commited index
	lastApplied		int			  // Highest index entry applied to state machine

	// Volatile state for leaders(Since every server could be leader)

	nextIndex 		[]int 		  // For each server, index of the next log entry to send
	matchIndex		[]int 		  // For each server, highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isleader
	DPrintf("In term %v, id %v isleader=%v\n",term,rf.me,isleader)
	return term, isleader
}
func (rf *Raft) GetEntry(index int) (int, Entry) {
	rank := index - rf.lastIncludedIndex - 1
	entry := Entry {
		Term	: rf.lastIncludedTerm,
		Index 	: rf.lastIncludedIndex,
	}
	if rank >= 0 && rank < len(rf.log){
		entry = rf.log[rank]
	}
	if rank == len(rf.log) {
		entry.Index = index
	}
	return rank, entry
}
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(cl int) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
	DPrintf("in line %v:server %v persist logged %v %v %v\n",cl,rf.me,rf.currentTerm,rf.votedFor,len(rf.log))
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	if data == nil || len(data) < 1 {
		return 
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int 
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int 
	var logs []Entry 
	
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&lastIncludedIndex) != nil ||
	   d.Decode(&lastIncludedTerm) != nil ||
	   d.Decode(&logs) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.log = logs
	}
	DPrintf("server %v recovered %v %v %v\n",rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	DPrintf("snapshot %v on %v",index, rf.me)
	if index < rf.lastIncludedIndex {
		return 
	}
	rank, entry := rf.GetEntry(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = entry.Term
	// DPrintf("%v log:%v",rf.me, rf.log)
	rf.log = rf.log[rank + 1:]
	rf.snapshot = snapshot
	rf.persist(207)
	rf.mu.Unlock()
	rf.applySnap()
}

// InstallSnapshot RPC arguments
type InstallSnapshotArgs struct {

	Term 				int
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm 	int
	Data				[]byte
}

type InstallSnapshotReply struct {
	Term 				int
}

func(rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm
	DPrintf("server %v recieve installsnap from %v",rf.me, args.LeaderId)
	if rf.currentTerm > args.Term {	
		return 
	}
	r := bytes.NewBuffer(args.Data)
	d := labgob.NewDecoder(r)
	var index int
	var commands []interface{}
	d.Decode(&index)
	d.Decode(&commands)
	DPrintf("installsnap: %v %v %v %v",args.LastIncludedIndex, args.LastIncludedTerm, index, commands)
	if index <= rf.lastIncludedIndex {
		return 
	}
	rf.mu.Lock()
	rf.snapshot = args.Data
	rank, entry := rf.GetEntry(args.LastIncludedIndex)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if entry.Term == args.LastIncludedTerm && rank < len(rf.log){
		rf.log = rf.log[rank + 1:]
		rf.mu.Unlock()	
		return 
	}
	rf.log = rf.log[len(rf.log):]
	rf.mu.Unlock()
	rf.applySnap()
	return 
}
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int     // Candidate's term
	CandidateId  int     // Candidate requesting vote
	LastLogIndex int     // Index of candidate's last log entry
	LastLogTerm  int     // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int		 // currentTerm for candidate updated itself
	VoteGranted  	bool 	 // true if the candidate recieves a vote
}

// appendentries RPC 
type AppendEntriesArgs struct {

	Term 		  	int		// leader's term
	LeaderId		int 	// redirect client request
	PrevLogIndex 	int 	// Last Log Index
	PrevLogTerm 	int		// term of prevlogindex
	Entries 		[]Entry // log entries to store, pherhaps more than one
	LeaderCommit 	int 	// leader's commit index
}

type AppendEntriesReply struct {

	Term 			int 
	IndexByPass		int
	Success			bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.applySnap()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return 
	} else if rf.currentTerm < args.Term {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.elect = false
		rf.votedFor = -1
		rf.isleader = false
		rf.persist(232)
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.elect = false
	DPrintf("%v request vote from %v lastlogindex:%v lastlogterm:%v log:%v %v\n",args.CandidateId, rf.me,args.LastLogIndex, args.LastLogTerm, len(rf.log), rf.votedFor)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId ){
		
		lastLogIndex := len(rf.log) + rf.lastIncludedIndex
		_, entry := rf.GetEntry(lastLogIndex)
		lastLogTerm := entry.Term
		
		// Check if candidate's log is at least as up-to-date as receiver's log
		if args.LastLogTerm > lastLogTerm || 
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.persist(241)
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	// DPrintf("%v(%v) recieved vote request from %v(%v), voted=%v\n",rf.me,rf.currentTerm, args.CandidateId,args.Term,reply.VoteGranted)

	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	DPrintf("in%v, args:%v log:%v term:%v\n",rf.me,args,len(rf.log),rf.currentTerm)
	rf.applySnap()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return 
	} else if rf.currentTerm < args.Term {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.elect = false
		rf.votedFor = -1
		rf.isleader = false
		rf.persist(275)
		rf.mu.Unlock()
	}
	
	// deal with heartbeat

	rf.mu.Lock()
	rf.elect = false
	// rf.currentTerm = args.Term 
	// rf.votedFor = args.LeaderId
	rf.mu.Unlock()
	// DPrintf("leadercommit:%v rfcommit:%v\n",args.LeaderCommit, rf.commitIndex)
	prevlogrank, prevlogentry := rf.GetEntry(args.PrevLogIndex)
	DPrintf("append prev:%v %v",prevlogrank, prevlogentry)
	if args.PrevLogIndex >= rf.lastIncludedIndex + 1 + len(rf.log) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return 
	} else if args.PrevLogIndex != -1 && prevlogentry.Term != args.PrevLogTerm {
		rf.mu.Lock()
		conflictterm := prevlogentry.Term
		rankbypass := prevlogrank - 1
		for(rankbypass >= 0 && rf.log[rankbypass].Term == conflictterm) {
			rankbypass-- 
		}
		reply.IndexByPass = rankbypass + rf.lastIncludedIndex + 1
		// DPrintf("%v Remove unsuitable index %v contains %v.",rf.me, args.PrevLogIndex, rf.log[args.PrevLogIndex])
		// rf.log = append(rf.log[:args.PrevLogIndex], nil...)
		// rf.log = rf.log[:args.PrevLogIndex]
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.persist(298)
		rf.mu.Unlock()
		return 
	} else {
		rf.mu.Lock()
		newlogindex := len(args.Entries) + args.PrevLogIndex
		if len(args.Entries) > 0 {
			newEntryIndex := args.PrevLogIndex + 1
			for i, entry := range args.Entries {
				rank, entry_rf := rf.GetEntry(newEntryIndex + i)
				if rank <  len(rf.log) {
					if entry_rf.Term != entry.Term {
						rf.log = rf.log[:rank]
						rf.log = append(rf.log, entry)
					}
				} else {
					rf.log = append(rf.log, entry)
				}
			}
			rf.persist(316)
		}
		// DPrintf("%v newlogindex: %v\n",rf.me, newlogindex)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > newlogindex{
				rf.commitIndex = newlogindex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			// rf.applierCond.Signal()
			go rf.applyComd()
		}
		rf.mu.Unlock()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	return 
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) 
	return ok 
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// index := -1
	// term := -1
	// isLeader := true

	rf.mu.Lock() 
	defer rf.mu.Unlock()

	if rf.isleader == false {
		return -1, rf.currentTerm, false
	}

	entry := Entry{
		Command: command,
		Term:	 rf.currentTerm,
		Index:   rf.lastIncludedIndex + len(rf.log) + 1,
	}
	rf.log = append(rf.log, entry)
	rf.persist(390)
	DPrintf("Command %v in Index %v recieved on server %v.", command, entry.Index, rf.me)
	go rf.replicateLog()
	// Your code here (2B).
	return rf.lastIncludedIndex + len(rf.log), rf.currentTerm, true
}

func (rf *Raft) replicateLog() {

	rf.mu.Lock()
	if rf.isleader == false {
		rf.mu.Unlock()
		return 
	}

	sendingTerm := rf.currentTerm
	rf.mu.Unlock()
	
	for peer := range rf.peers {
		if peer != rf.me {
			go func (Id int) {
				for !rf.killed() && sendingTerm == rf.currentTerm{
					rf.mu.Lock()
					if rf.isleader == false {
						// DPrintf("%v no longer a leader, replicated ends.",rf.me)
						rf.mu.Unlock()
						return 
					}
					if rf.nextIndex[Id] <= rf.lastIncludedIndex {
						args := InstallSnapshotArgs {
							Term 				: sendingTerm,
							LeaderId			: rf.me,
							LastIncludedIndex	: rf.lastIncludedIndex,
							LastIncludedTerm	: rf.lastIncludedTerm,
							Data				: rf.snapshot,
						}
						rf.mu.Unlock()
						reply := InstallSnapshotReply{}
						if rf.sendInstallSnapshot(Id, &args, &reply) {
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.elect = false
								rf.votedFor = -1
								rf.isleader = false
								rf.persist(536)
								rf.mu.Unlock()
								return 
							}
							if (rf.lastIncludedIndex + 1 > rf.nextIndex[Id]) {
								rf.nextIndex[Id] = rf.lastIncludedIndex + 1
							}
							rf.mu.Unlock()
							continue
						}
						rf.mu.Lock()
					}
					currentNextIndex := rf.nextIndex[Id]
					nextIndex, entry := rf.GetEntry(rf.nextIndex[Id])
					DPrintf("%v -> %v nextIndex: %v entry: %v nextindex: %v log:%v",rf.me, Id, nextIndex, entry, rf.nextIndex[Id],len(rf.log))
					prevLogIndex := entry.Index - 1
					_, entry_prev := rf.GetEntry(prevLogIndex)
					prevLogTerm := entry_prev.Term
					entries := make([]Entry, 0)
					if nextIndex < len(rf.log) && nextIndex >= 0{
						entries = rf.log[nextIndex:]
					}
					
					args := AppendEntriesArgs{
						Term		   : sendingTerm,
						LeaderId	   : rf.me,
						PrevLogIndex   : prevLogIndex,
						PrevLogTerm	   : prevLogTerm,
						Entries		   : entries,
						LeaderCommit   : rf.commitIndex,
					}
					rf.mu.Unlock()

					reply := AppendEntriesReply{}
					
					if rf.sendAppendEntries(Id, &args, &reply) {
						rf.mu.Lock()
						DPrintf("peer %v reply %v\n",Id, reply)
						if reply.Success {
							if rf.nextIndex[Id] < currentNextIndex + len(entries){
								rf.nextIndex[Id] = currentNextIndex + len(entries)
							}
							rf.matchIndex[Id] = rf.nextIndex[Id] - 1

							rf.tryCommitLogs()
							rf.mu.Unlock()
							return 
						} else {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.elect = false
								rf.votedFor = -1
								rf.isleader = false
								rf.persist(459)
								rf.mu.Unlock()
								return 
							}
							// To be optimized, skip all logs in the same term
							rf.nextIndex[Id] = reply.IndexByPass
							DPrintf("mismatched for leader %v follower %v, new nextIndex is %v",rf.me, Id, rf.nextIndex[Id])
							rf.mu.Unlock()
							continue
						} 
						rf.mu.Unlock()
						return 
					}
				}
			}(peer)
		}
	}
}
func (rf *Raft) tryCommitLogs() {
	
	rf.applySnap()

	for index := rf.commitIndex + 1; index < rf.lastIncludedIndex + 1 + len(rf.log); index ++ {
		_, entry := rf.GetEntry(index)
		if entry.Term != rf.currentTerm {
			continue
		}
		// DPrintf("Index %v",index)
		count := 1

		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= index {
				count ++
			}
		}
		// DPrintf("Commited Vote %v", count)
		if count + count > len(rf.peers) {
			// DPrintf("commitindex: %v index: %v",rf.commitIndex, index)
			
			rf.commitIndex = index

			// rf.applierCond.Signal()
			go rf.applyComd()
		}
	}
}
func (rf *Raft) applySnap() {

	if rf.lastApplied >= rf.lastIncludedIndex {
		return 
	}
	DPrintf("applied on %v, index %v, now applied %v",rf.me, rf.lastIncludedIndex, rf.lastApplied)
	msg := ApplyMsg {
		SnapshotValid	:true,
		Snapshot		:rf.snapshot,
		SnapshotTerm	:rf.lastIncludedTerm,
		SnapshotIndex	:rf.lastIncludedIndex,
	}
	rf.applyCh <- msg
	rf.mu.Lock()
	if rf.lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	rf.lastApplied = rf.lastIncludedIndex
	rf.mu.Unlock()
}
func (rf *Raft) applyComd() {
	rf.applySignalCh <- applySignal{}
}
func (rf *Raft) applierLoop () {
	for !rf.killed() {
		<-rf.applySignalCh
		rf.applyComdFunc()
	}
}
func (rf *Raft) applyComdFunc() {
	if rf.lastApplied >= rf.commitIndex {
		return 
	}
	rf.applySnap()
	rf.mu.Lock()
	newlast := rf.commitIndex
	prelast := rf.lastApplied + 1
	// DPrintf("%v %v %v\n", rf.me, rf.commitIndex, rf.lastApplied)
	entries := make([]Entry, rf.commitIndex - rf.lastApplied)
	index1, _ := rf.GetEntry(rf.lastApplied + 1)
	index2, _ := rf.GetEntry(rf.commitIndex + 1)
	applytaskid := rand.Int63()
	DPrintf("%v", rf.lastIncludedIndex)
	DPrintf("ID:%v entries from %v(%v) - %v(%v) to be commited on %v\n",applytaskid, index1, rf.lastApplied + 1,index2, rf.commitIndex + 1,rf.me)
	copy(entries, rf.log[index1 : index2])
	rf.lastApplied = newlast 
	rf.mu.Unlock()
	// successfulapplied := prelast - 1
	for i, entry := range entries {
		msg := ApplyMsg{
			CommandValid: true,
			Command: 	  entry.Command,
			CommandIndex: prelast + i,
		}
		DPrintf("ID:%v msg to be applied:%v",applytaskid,msg)
		rf.applyCh <- msg
		// successfulapplied = prelast + i
	}
	// DPrintf("%v %v %v\n", rf.me, rf.commitIndex, rf.lastApplied)
}
func (rf *Raft) HeartBeater() {
	for rf.killed() == false {
		rf.sendHeartbeats()
		time.Sleep(time.Duration(125) * time.Millisecond)
	}
}
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.isleader == false {
		rf.mu.Unlock()
		return 
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	// DPrintf("%v begins beat.", rf.me)
	// followers := make(chan bool, len(rf.peers) - 1)
	
	for i := range rf.peers {
		if i != rf.me {
			go func(id int) {
				nextIndex := rf.nextIndex[id]
				prevLogIndex := nextIndex - 1
				_, entry := rf.GetEntry(prevLogIndex)
				prevLogTerm := entry.Term
				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				args := AppendEntriesArgs {
					Term 			: currentTerm,
					LeaderId 		: rf.me,
					PrevLogIndex	: prevLogIndex,
					PrevLogTerm		: prevLogTerm,
					LeaderCommit	: rf.commitIndex,
				}
				reply := AppendEntriesReply {}			
				if rf.sendAppendEntries(id, &args, &reply) {
					if reply.Term > rf.currentTerm{
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.isleader = false
						rf.votedFor = -1 
						rf.persist(574)
						rf.mu.Unlock()
						return 
					}
				}
			}(i)
		} 
	}
}
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// DPrintf("server %v in term %v be killed.", rf.me, rf.currentTerm)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) election() {
	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	// become candidate
	rf.votedFor = rf.me
	rf.isleader = false
	rf.persist(623)
	rf.mu.Unlock()
	rf.applySnap()
	ms := 150 + (rand.Int63() % 150)
	electionTimer := time.NewTimer(time.Duration(ms) * time.Millisecond)
	voted := 1 // voted for self
	counter := make(chan bool, len(rf.peers) - 1)
	DPrintf("%v in term %v invoke election.",rf.me,rf.currentTerm)
	for i := range rf.peers {
		if i != rf.me {
			go func(id int) {
				temp := len(rf.log) - 1
				lastLogIndex := 0
				lastLogTerm := 0
				if temp >= 0 {
					lastLogTerm = rf.log[temp].Term
					lastLogIndex = rf.log[temp].Index
				} else {
					lastLogIndex = rf.lastIncludedIndex
					lastLogTerm = rf.lastIncludedTerm
				}
				args := RequestVoteArgs {
					Term         :rf.currentTerm,  
					CandidateId  :rf.me,
					LastLogIndex :lastLogIndex,
					LastLogTerm  :lastLogTerm,
				}
				reply := RequestVoteReply {}
				
				ok := rf.sendRequestVote(id, &args, &reply)
				if ok == true {
					rf.mu.Lock()
					// DPrintf("%v found %v well.\n",rf.me, id)
					counter <- reply.VoteGranted
					rf.mu.Unlock()
				} else {
					// DPrintf("%v fail to find %v\n",rf.me, id)
					counter <- false
				}
			}(i)
		}
	}
	
	for i := 0; i < len(rf.peers) - 1; i++{
		select {
		case result := <- counter:
			if result == true {
				voted ++
				if voted + voted > len(rf.peers) && rf.elect == true { // check if it have become leader
					rf.mu.Lock()
					rf.isleader = true 
					// initialize the nextindex and matchindex
					for t := 0; t < len(rf.peers); t++ {
						rf.nextIndex[t] = rf.lastIncludedIndex + 1 + len(rf.log)
						rf.matchIndex[t] = -1
					}
					// DPrintf("%v in term %v recieved %v votes\n",rf.me, rf.currentTerm, voted)
					rf.mu.Unlock()
					return 
				}
			}
		case <-electionTimer.C:
			return
		}
		
	}
	// DPrintf("%v in term %v, election ends.",rf.me,rf.currentTerm)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
					
		if rf.elect == true && rf.isleader == false {
			rf.election()
		}
		rf.mu.Lock()
		if rf.isleader == false {
			rf.elect = true
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 150 and 350
		// milliseconds. Since the tester could only accept 10 
		// times heartbeats per second
		ms := 150 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		
	}
}
func (rf *Raft) applyEntries() {
	if rf.lastApplied < rf.lastIncludedIndex {
        snapshot := rf.snapshot
        snapshotTerm := rf.lastIncludedTerm
        snapshotIndex := rf.lastIncludedIndex
        rf.mu.Unlock()
        
        msg := ApplyMsg {
            SnapshotValid: true,
            Snapshot: snapshot,
            SnapshotTerm: snapshotTerm,
            SnapshotIndex: snapshotIndex,
        }
        
        // Try to send the snapshot
        rf.applyCh <- msg
        
        // Update lastApplied after successful application
        rf.mu.Lock()
        if rf.lastApplied < snapshotIndex {
            rf.lastApplied = snapshotIndex
        }
        if rf.commitIndex < snapshotIndex {
            rf.commitIndex = snapshotIndex
        }
    }

	newlast := rf.commitIndex
	prelast := rf.lastApplied + 1

	entries := make([]ApplyMsg, 0)
	for i := prelast; i <= newlast; i++ {
		rank, entry := rf.GetEntry(i)
		if rank >= 0 && rank < len(rf.log) {
			msg := ApplyMsg{
				CommandValid	: true,
				Command			: entry.Command,
				CommandIndex	: i,
			}
			entries = append(entries, msg)
		}
	}
	applytaskid := rand.Int63()
	DPrintf("ID:%v entries from %v - %v to be commited on %v\n",applytaskid, rf.lastApplied + 1, rf.commitIndex + 1,rf.me)
	rf.mu.Unlock()

	lastSuccessfulIndex := rf.lastApplied
	for _, msg := range entries {
		DPrintf("ID:%v msg to be applied:%v on %v",applytaskid,msg,rf.me)
		rf.applyCh <- msg
		lastSuccessfulIndex = msg.CommandIndex
	}
	rf.mu.Lock()
	if lastSuccessfulIndex > rf.lastApplied {
        rf.lastApplied = lastSuccessfulIndex
    }
}
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("server %v recoverd/begins.\n",me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = append(rf.log, Entry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.randomElectionTimeout = 150 + (rand.Int63() % 150)
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1
	rf.applierCond = sync.NewCond(&rf.mu)
	rf.applierActive = false
	rf.applySignalCh = make(chan applySignal)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	
	// start ticker goroutine to start elections
	go rf.HeartBeater()
	go rf.ticker()
	go rf.applierLoop()


	return rf
}
