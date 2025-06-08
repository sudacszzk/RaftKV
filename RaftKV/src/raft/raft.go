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

//matchindex在appendentry收到success时改变
//leader的commitindex在matchindex排序后取中间的 follower的commitindex在appendentry时改变

import (
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"bytes"
	"6.824/labgob"
)

type NodeState uint8


const (
	//HeartbeatTimeout = 125
	HeartbeatTimeout = 10
	ElectionTimeout  = 1000
)

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}


const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	//r:=rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	CommandTerm  int
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}

type AppendEntriesResponse struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries
	state          NodeState

	currentTerm int
	votedFor    int
	logs        []Entry // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer


}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	DPrintf("trying to lock %v in getstate",rf.me)
	rf.mu.Lock()
	DPrintf("have lock %v in getstate",rf.me)
	//defer rf.mu.Unlock()
	var term int
	var isleader bool
	term=rf.currentTerm
	isleader=(rf.state==StateLeader)
	rf.mu.Unlock()
	DPrintf("have released lock %v in getstate",rf.me)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	/*
	currentTerm int
	votedFor    int
	logs        []Entry // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command
	*/
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var xxx int
	var yyy int
	var zzz []Entry
	if d.Decode(&xxx) != nil ||d.Decode(&yyy) != nil ||d.Decode(&zzz) != nil {

	} else {
	   rf.currentTerm = xxx
	   DPrintf("in read: term: %v",rf.currentTerm)
	   rf.votedFor = yyy
	   rf.logs = zzz
	}

}


func (rf *Raft) StartElection() {
	
	rf.currentTerm = rf.currentTerm+1
	rf.votedFor=rf.me
	grantedVotes:=1
	DPrintf("start election candidate:%v term:%v",rf.me,rf.currentTerm)
	rf.ChangeState(StateCandidate)

	lastLog := rf.getLastLog()
	request := &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			DPrintf("start asking vote. me: %v peer %v",rf.me,peer)
			
			response := new(RequestVoteResponse)
			if rf.sendRequestVote(peer, request, response) {
				DPrintf("trying to lock %v in startelection",rf.me)
				rf.mu.Lock()
				DPrintf("have lock %v in startelection",rf.me)
				//defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response, peer, request, rf.currentTerm)
				if rf.currentTerm == request.Term && rf.state == StateCandidate {
					if response.VoteGranted {
						DPrintf("{Node %v} receives vote from %v in term %v", rf.me, peer, rf.currentTerm)
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(StateLeader)
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
						rf.currentTerm, rf.votedFor = response.Term, -1
						//DPrintf("in startelection2: term: %v",rf.currentTerm)
						rf.persist()
					}
				}
				rf.mu.Unlock()
				DPrintf("have released lock %v in startelection",rf.me)
			}
		}(peer)//go语言中函数定义结尾的小括号就是在调用这个函数
		
	}



	
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) ChangeState(state NodeState) {
	DPrintf("node %v changing from %v to %v ",rf.me,rf.state,state)
	rf.state=state
	if(state==StateLeader){
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())

	}else if (state==StateCandidate){

	}else{
		//follower
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomizedElectionTimeout())

	}

	
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}


type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {
	
	DPrintf("trying to lock %v in appendentries",rf.me)
	rf.mu.Lock()
	DPrintf("have lock %v in appendentries",rf.me)
	if(rf.currentTerm>request.Term){
		rf.mu.Unlock()
		return
	}
	//defer rf.mu.Unlock()
	//defer rf.persist()
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	if(rf.currentTerm<=request.Term){
		rf.ChangeState(StateFollower)
		rf.currentTerm=request.Term
	}
	//rf.ChangeState(StateFollower)
	DPrintf("appendentry from %v to %v",request.LeaderId,rf.me)

	rf.logs=rf.logs[:1]
	for i:=1;i<len(request.Entries);i++{
		rf.logs=append(rf.logs,request.Entries[i])
		
		
	}
	response.Success=true
	/*
	for i:=1;i<len(request.Entries);i++{
		if (len(rf.logs)>=request.Entries[i].Index+1){
			rf.logs[request.Entries[i].Index]=rf.logs[i]
		}else{
			rf.logs=append(rf.logs,request.Entries[i])
		}
		
	}
	*/
	rf.commitIndex=len(request.Entries)-1
	//rf.matchIndex=len(request.Entries)-1
	DPrintf2("%v follower:%v",rf.logs,rf.me)
	DPrintf2("%v leader:%v",request.Entries,request.LeaderId)

	rf.persist()
	rf.mu.Unlock()
	DPrintf("released lock %v in appendentries",rf.me)
}
//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
func (rf *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) {
	DPrintf("trying to lock %v in requestvote",rf.me)
	rf.mu.Lock()
	DPrintf("have lock %v in requestvote",rf.me)
	//defer rf.mu.Unlock()
	//defer rf.persist()
	/*
	if request.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	*/
	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
	//if request.Term < rf.currentTerm || ( rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		rf.mu.Unlock()
		return
	}
	
	if request.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = request.Term, -1
		//DPrintf("in requestvote: term: %v",rf.currentTerm)
	}
	/*
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	*/
	rf.votedFor = request.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	//DPrintf("reset electionntime %v",rf.me)
	response.Term, response.VoteGranted = rf.currentTerm, true
	
	rf.persist()
	rf.mu.Unlock()
	DPrintf("have released lock %v in requestvote",rf.me)
	
	
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
//


func (rf *Raft) sendRequestVote(server int, request *RequestVoteRequest, response *RequestVoteResponse) bool {
	return rf.peers[server].Call("Raft.RequestVote", request, response)
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	return rf.peers[server].Call("Raft.AppendEntries", request, response)
}

func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %d} advance commitIndex from %d to %d with leaderCommit %d in term %d", rf.me, rf.commitIndex, newCommitIndex, leaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		//rf.applyCond.Signal()
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("trying to lock %v in start",rf.me)
	rf.mu.Lock()
	DPrintf("have lock %v in start",rf.me)
	//defer rf.mu.Unlock()
	DPrintf("Start %v",rf.me)
	if(rf.state!=StateLeader){
		rf.mu.Unlock()
		return -1,-1,false
	}
	rf.logs=append(rf.logs,Entry{len(rf.logs), rf.currentTerm, command})
	//rf.commitIndex=len(rf.logs)-1
	rf.persist()
	rf.mu.Unlock()
	DPrintf("have released lock %v in start",rf.me)
	return len(rf.logs)-1,rf.currentTerm,true
}



func (rf *Raft) replicator(peer int) {
	/*
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer)
	}
	*/
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[peer] - 1
		firstIndex := rf.getFirstLog().Index
		entries := make([]Entry, len(rf.logs[prevLogIndex+1-firstIndex:]))
		copy(entries, rf.logs[prevLogIndex+1-firstIndex:])

		request := &AppendEntriesRequest{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			//Entries:	  entries,
			Entries:	  rf.logs,
			LeaderCommit: rf.commitIndex,
		}
		
		response := new(AppendEntriesResponse)
		go rf.sendAppendEntries(peer, request, response)
		
		
	}
}
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	fl:=0
	for rf.killed() == false {
		fl=fl+1
		//DPrintf("while not killed %v %v %v fl:%v",rf.me,rf.currentTerm,rf.state,fl)
		select {
		case <-rf.electionTimer.C:
			//DPrintf("before election term: %v fl: %v",rf.currentTerm,fl)
			DPrintf("trying to lock %v before election",rf.me)
			rf.mu.Lock()
			DPrintf("have lock %v before election",rf.me)
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			DPrintf("have released lock %v before election",rf.me)
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			//DPrintf("now leader: %v term: %v state:%v fl: %v",rf.me, rf.currentTerm,rf.state,fl)
			DPrintf("trying to lock %v in heartbeat",rf.me)
			rf.mu.Lock()
			DPrintf("have lock %v in heartbeat",rf.me)
			if rf.state == StateLeader {
				
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			DPrintf("have released lock %v in heartbeat",rf.me)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		/*
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		*/
		if (rf.lastApplied >= rf.commitIndex){
			rf.mu.Unlock()
			continue
		}
		DPrintf2("me: %v commitIndex: %v",rf.me,rf.commitIndex)
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		if (commitIndex>rf.lastApplied){
			rf.lastApplied=commitIndex
		}
		//rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
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
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),//leader发给每个server的下一条log的index
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("rf  me: %v  currentTerm: %v state:%v ",rf.me,rf.currentTerm,rf.state)
	
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			//go rf.replicator(i)
		}
	}
	
	// start ticker goroutine to start elections
	
	go rf.ticker()
	
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}