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
import "labrpc"
import "time"
import "math/rand"
import "fmt"
import "bytes"
import "labgob"


// 2A part : set const variable
const Follower = 1
const Candidate = 2
const Leader = 3

const electionTimeoutLower = time.Duration(300)*time.Millisecond
const electionTimeoutUpper = time.Duration(400)*time.Millisecond
const heartbeatInterval = time.Duration(120)*time.Millisecond



// 2A part : return a random election timeout
func randElectionTimeout() time.Duration{
	num := rand.Int63n(electionTimeoutUpper.Nanoseconds()-electionTimeoutLower.Nanoseconds()) + electionTimeoutLower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ƒ, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 3B send snapshot to 
	CommandData []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int 	//2A part:latest term
	votedFor int 		//2A part:candidateId that received vote in current term
	state int 			//2A part:server's current state
	electionTimer *time.Timer 	//2A part: keep track of election timeout
	heartbeatTimer *time.Timer 	//2A part: keep track of heartbeat timeout

	log []Entry 		//2B part:log entries;
	commitIndex int 	//2B part:index of highest log entry known to be commited
	lastApplied int 	//2B part:index of highest log entry applied to state machine

	nextIndex []int 	//2B part:for each server,index of the next log entry to send to that sever
	matchIndex []int 	//2B part:for each server,index of highest log entry known to be replicated on server

	applyCh chan ApplyMsg 	//2B part:a channel provided to apply Command to state machine

	snapshottedIndex int //3B part: the last index which has been snapshotted
}

type Entry struct{
	Term int 
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}



func (rf *Raft) encodeRaftState() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshottedIndex)
	e.Encode(rf.log)
	return w.Bytes()

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
	
	rf.persister.SaveRaftState(rf.encodeRaftState())
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
	var currentTerm int
	var votedFor int
	var snapshottedIndex int
	var log []Entry
	if d.Decode(&currentTerm) != nil || 
		d.Decode(&votedFor) != nil || 
		  d.Decode(&snapshottedIndex) != nil|| d.Decode(&log)!= nil {
			fmt.Println("readPersist fail!")
	}else{

		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.snapshottedIndex = snapshottedIndex
		rf.log = log

		rf.commitIndex = snapshottedIndex
		rf.lastApplied = snapshottedIndex
	}

}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int 			//2A part:candidate's term
	CandidateId int 	//2A part:candidate requesting vote

	LastLogIndex int 	//2B part:index of candidate's last log entry
	LastLogTerm int 	//2B part:term of candidate's last log entry

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 		//2A part:currentTerm,for candidate to update itself
	VoteGranted bool 	//2A part:true means cnadidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A part:rf server receive vote request and reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId){
		reply.VoteGranted = false
		return 
	}

	// 2B part:rf willing to vote to a candidate which log at least as up-to-date as self log
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.changeState(Follower)
	}

	lastLogIndex := len(rf.log)-1
	if args.LastLogTerm < rf.log[lastLogIndex].Term || (args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < (lastLogIndex + rf.snapshottedIndex)){
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

	// reset election timer to avoid rf start election
	rf.electionTimer.Reset(randElectionTimeout())
	return 
}



// 2A part:creating AppendEntries structure following RequestVote structure
//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term int 		//2A part:leader's term
	LeaderId int 	//2A part:so follower can redirect clients

	PrevLogIndex int 	//2B part:index of log entry immediately preceding new ones
	PrevLogTerm int 	//2B part:term of prevLogIndex entry 
	Entries []Entry 	//2B part:log entries to store
	LeaderCommit int 	//2B part:leader's commitIndex

}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term int 			//2A part:currentTerm,for leader to update itself
	Success bool 		//2A part:true means cnadidate received vote

	ConflictTerm int 	//2C part:term of ConflictEntry
	ConflictIndex int 	//2C part:the first index in ConflictTerm
}



//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// 2A part:rf server receive heartbeat and reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.Success = false
		return
	}

	
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.changeState(Follower)
	}

	// Reset election timer
	rf.electionTimer.Reset(randElectionTimeout())


	if args.PrevLogIndex <= rf.snapshottedIndex{
		reply.Success = true

		// sync log if needed
		if args.PrevLogIndex + len(args.Entries) > rf.snapshottedIndex{
			// if snapshottedIndex == prevLogIndex, all log entries should be added
			startIdx := rf.snapshottedIndex - args.PrevLogIndex
			// only keep the last snapshotted one
			rf.log = rf.log[:1]
			rf.log = append(rf.log,args.Entries[startIdx:]...)
		}
		return 
	}
	// Your code here (2A, 2B).
	// 2B part:rf server receive AppendEntries and check log
	absoluteLastLogIndex := len(rf.log)-1 + rf.snapshottedIndex
	if absoluteLastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = absoluteLastLogIndex +1
		reply.ConflictTerm = -1
		return
	}

	if rf.log[rf.getRelativeLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm{
		reply.Success = false
		reply.ConflictTerm = rf.log[rf.getRelativeLogIndex(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for rf.log[rf.getRelativeLogIndex(i-1)].Term == reply.ConflictTerm{
			i--
			if i == rf.snapshottedIndex +1{
				// this may happen after snapshpt
				// because the term of the first log may be the current term
				// before lab 3b this is not going to happen , since rf.log[0].term = 0
				break
			}
		}
		reply.ConflictIndex = i
		return 
	}
	

	// // compare from rf.logs[args.PrevLogIndex + 1]
	// unmatch_idx := -1
	// for idx := range args.Entries {
	// 	if len(rf.log) < rf.getRelativeLogIndex(args.PrevLogIndex+2+idx) ||
	// 		rf.log[rf.getRelativeLogIndex(args.PrevLogIndex+1+idx)].Term != args.Entries[idx].Term {
	// 		// unmatch log found
	// 		unmatch_idx = idx
	// 		break
	// 	}
	// }

	// if unmatch_idx != -1 {
	// 	// there are unmatch entries
	// 	// truncate unmatch Follower entries, and apply Leader entries
	// 	rf.log = rf.log[:rf.getRelativeLogIndex(args.PrevLogIndex+1+unmatch_idx)]
	// 	rf.log = append(rf.log, args.Entries[unmatch_idx:]...)
	// }


	rf.log = rf.log[:rf.getRelativeLogIndex(args.PrevLogIndex+1)]
	rf.log = append(rf.log,args.Entries...)
	// unmatch_idx := -1
	// for idx := range args.Entries {
	// 	if len(rf.log) < rf.getRelativeLogIndex(args.PrevLogIndex+2+idx) ||
	// 		rf.log[rf.getRelativeLogIndex(args.PrevLogIndex+1+idx)].Term != args.Entries[idx].Term {
	// 		// unmatch log found
	// 		unmatch_idx = idx
	// 		break
	// 	}
	// }

	// if unmatch_idx != -1 {
	// 	// there are unmatch entries
	// 	// truncate unmatch Follower entries, and apply Leader entries
	// 	rf.log = rf.log[:rf.getRelativeLogIndex(args.PrevLogIndex+1+unmatch_idx)]
	// 	rf.log = append(rf.log, args.Entries[unmatch_idx:]...)
	// }

	if args.LeaderCommit > rf.commitIndex{
		lastLogIndex := len(rf.log) -1 + rf.snapshottedIndex
		if args.LeaderCommit > lastLogIndex{
			rf.setCommit(lastLogIndex)
		}else{
			rf.setCommit(args.LeaderCommit)
		}
	}

	reply.Success = true
	return
}


func (rf *Raft)setCommit(commitIndex int){
	rf.commitIndex = commitIndex

	if rf.commitIndex > rf.lastApplied{
		go func(start int,entries []Entry){
			for idx,entry := range entries{
				msg := ApplyMsg{
					CommandValid : true,
					Command : entry.Command,
					CommandIndex : start+idx,
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex{
					rf.lastApplied = msg.CommandIndex
				}
				// fmt.Printf(" %d send %v cmd to commit\n",rf.me,entry.Command)
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1,rf.log[rf.getRelativeLogIndex(rf.lastApplied+1) :rf.getRelativeLogIndex(rf.commitIndex+1)])
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


// 2A part: write a function to change server state
// due to rf info may changed, this func need be called with a lock
func (rf *Raft) changeState(s int){
	if rf.state == s{
		return
	}
	rf.state = s

	switch s{
	case Follower:
		rf.votedFor = -1
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randElectionTimeout())

	case Candidate:
		rf.startElection()

	case Leader:
		for i:= range rf.nextIndex{
			rf.nextIndex[i] = len(rf.log) + rf.snapshottedIndex
		}
		for i:= range rf.matchIndex{
			rf.matchIndex[i] = rf.snapshottedIndex
		}

		rf.electionTimer.Stop()
		rf.broadcastEntries()
		rf.heartbeatTimer.Reset(heartbeatInterval)
	}
}


// 2A part: write a function to help candidate begin an election
func (rf *Raft) startElection(){
	defer rf.persist()
	rf.currentTerm ++
	// fmt.Printf("%d begin election ,term : %d\n",rf.me,rf.currentTerm)
	rf.electionTimer.Reset(randElectionTimeout())

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term : rf.currentTerm,
		CandidateId : rf.me,
		LastLogIndex : lastLogIndex+rf.snapshottedIndex,
		LastLogTerm : rf.log[lastLogIndex].Term,
	}

	// 2A part: candidate vote for self
	count := 1
	rf.votedFor = rf.me

	for i := range rf.peers{
		if(i == rf.me){
			continue
		}

		go func(peer int){
			var reply RequestVoteReply
			if rf.sendRequestVote(peer,&args,&reply){
				rf.mu.Lock()
				if reply.VoteGranted && rf.state == Candidate {
					count ++
					if(count > len(rf.peers)/2){
						rf.changeState(Leader)
					}
				}else{
					if reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.changeState(Follower)
						rf.persist()
					}
				}
				rf.mu.Unlock()
			}
		}(i)

	}
}




// 2A part: write a function to broadcast periodically
func (rf *Raft) broadcastEntries(){
	for i := range rf.peers{
		if i == rf.me {
			continue
		}
		go func(peer int){	
			rf.mu.Lock()
			if rf.state != Leader{
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[peer]-1

			if prevLogIndex < rf.snapshottedIndex{
				rf.mu.Unlock()
				rf.syncSnapshotWith(peer)
				return 
			}

			entries := make([]Entry,len(rf.log[rf.getRelativeLogIndex(prevLogIndex+1):]))
			copy(entries,rf.log[rf.getRelativeLogIndex(prevLogIndex+1):])

			args := AppendEntriesArgs{
				Term : rf.currentTerm,
				LeaderId : rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm : rf.log[rf.getRelativeLogIndex(prevLogIndex)].Term,
				Entries : entries,
				LeaderCommit : rf.commitIndex,
			} 
			rf.mu.Unlock()

			var reply AppendEntriesReply
			if rf.sendAppendEntries(peer,&args,&reply){
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if reply.Success{
					rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peer] = rf.matchIndex[peer] +1

					// traversal entry through rf.log find entries can be committed
					// if there exists an N such that N > commitIndex, a majority of matchIndex[i]>=N
					// and log[N].term == currentTerm: set commitIndex = N
					for i:=len(rf.log)-1 + rf.snapshottedIndex;i >rf.commitIndex && rf.currentTerm == rf.log[i-rf.snapshottedIndex].Term;i--{
						count := 0
						for _,matchIndex := range rf.matchIndex{
							if matchIndex >= i{
								count ++
							}
						}

						if count > len(rf.peers)/2{
							rf.setCommit(i)
							break
						}
					}


				}else{
					if reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.changeState(Follower)
						rf.persist()
					}else{
						rf.nextIndex[peer] = reply.ConflictIndex
						if reply.ConflictTerm != -1{
							for i := args.PrevLogIndex;i>=rf.snapshottedIndex+1;i--{	// why >= 1?
								if rf.log[rf.getRelativeLogIndex(i-1)].Term == reply.ConflictTerm{
									rf.nextIndex[peer] = i
									break
								}
							}
						}
					}
				}
				rf.mu.Unlock()
			}
		}(i)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term,isLeader = rf.GetState()
	if isLeader{
		rf.mu.Lock()
		index = len(rf.log) + rf.snapshottedIndex
		rf.log = append(rf.log,Entry{Term:term,Command:command})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index+1
		rf.persist()
		// fmt.Printf("Leader %d apply %v cmd\n",rf.me,command)
		rf.broadcastEntries()
		rf.mu.Unlock()
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


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
//broad have the same order. persister is a place for this server to
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

	// Your initialization code here (2A, 2B, 2C).
	// 2A part: initialize currentTerm,votedFor,state,electionTimer,heartbeatTimer
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.electionTimer = time.NewTimer(randElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(heartbeatInterval)


	// 2B part: initialize log,commitIndex,lastApplied,nextIndex,matchIndex,applyCh
	rf.applyCh = applyCh
	rf.log = make([]Entry,1) 	//first index is 1,in case of traversal overflow
	
	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	rf.nextIndex = make([]int,len(rf.peers))
	for i:= range rf.nextIndex{
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int,len(rf.peers))
	
	// fmt.Printf("server %d restart and persist\n",rf.me)

	// creating a background goroutine that will listen electionTimer and heartbeatTimer
	go func(node *Raft){
		for{
			select{
			case <- rf.electionTimer.C:
				rf.mu.Lock()
				// fmt.Printf("%d election timeout\n",rf.me)
				if rf.state == Follower{
					rf.changeState(Candidate)
				}else{
					rf.startElection()
				}
				rf.mu.Unlock()

			case <- rf.heartbeatTimer.C:
				rf.mu.Lock()
				// fmt.Printf("%d receive hearbeat\n",rf.me)
				if rf.state == Leader{
					rf.broadcastEntries()
					rf.heartbeatTimer.Reset(heartbeatInterval)
				}
				rf.mu.Unlock()
			}
		}
	}(rf)

	return rf
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// get the index of really store in log
func (rf *Raft) getRelativeLogIndex(i int) int{
	return i-rf.snapshottedIndex
}

// leader truncate self log and send truncate request to other Followers
func(rf *Raft) ReplaceLogWithSnapshot(appliedIndex int,kvSnapshot []byte){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if appliedIndex <= rf.snapshottedIndex{
		return
	}

	// truncate log ,keep snapshottedIndex as a guard at rf.log[0]
	rf.log = rf.log[rf.getRelativeLogIndex(appliedIndex):]
	// fmt.Printf("%d begin to trim from %d to %d\n",rf.me,rf.snapshottedIndex,appliedIndex)
	rf.snapshottedIndex = appliedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(),kvSnapshot)

	// send other Follower truncate log request
	for i := range rf.peers{
		if i == rf.me{
			continue
		}
		go rf.syncSnapshotWith(i)
	} 
}


// invoke by Leader to sync snapshot with one follower
func (rf *Raft) syncSnapshotWith(server int){
	rf.mu.Lock()
	if rf.state != Leader{
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term : rf.currentTerm,
		LeaderId : rf.me,
		LastIncludedIndex : rf.snapshottedIndex,
		LastIncludeTerm: rf.log[0].Term,
		Data : rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()

	var reply InstallSnapshotReply

	if rf.sendInstallSnapshot(server,&args,&reply){
		rf.mu.Lock()
		if reply.Term > rf.currentTerm{
			rf.currentTerm = reply.Term
			rf.changeState(Follower)
			rf.persist()
		}else{
			if rf.matchIndex[server] < args.LastIncludedIndex{
				rf.matchIndex[server] = args.LastIncludedIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
		rf.mu.Unlock()
	}
}

// 3B
// send snapshot RPC to follower
type InstallSnapshotArgs struct{
	Term int  
	LeaderId int 
	LastIncludedIndex int
	LastIncludeTerm int
	Data []byte
}

type InstallSnapshotReply struct{
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// do not need to call rf.persist() in this function
	// because rf.persister.SaveStateAndSnapshot() is called

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.snapshottedIndex{
		return
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.changeState(Follower)
	}

	// if existing log entry has same index and term with
	// last log entry in snapshot, retain log entries following it
	lastIncludedRelativeIndex := rf.getRelativeLogIndex(args.LastIncludedIndex)
	if len(rf.log) > lastIncludedRelativeIndex && rf.log[lastIncludedRelativeIndex].Term == args.LastIncludeTerm{
		rf.log = rf.log[lastIncludedRelativeIndex:]
	}else{
		// discard entire log and force correct log
		rf.log = []Entry{{Term:args.LastIncludeTerm,Command:nil}}
	}

	// 5.save snapshot file,discard any existing snapshot
	rf.snapshottedIndex = args.LastIncludedIndex

	//!!!IMPORTANT
	// update commitIndex and lastApplied because after sync
	// snapshot,it has at least applied all logs before snapshottedIndex
	// and directly save to state machine
	if rf.commitIndex < rf.snapshottedIndex{
		rf.commitIndex = rf.snapshottedIndex
	}

	if rf.lastApplied < rf.snapshottedIndex{
		rf.lastApplied = rf.snapshottedIndex
	}

	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(),args.Data)

	if rf.lastApplied > rf.snapshottedIndex{
		// snapshot is elder than kvserver database
		return 
	}

	installsnapshotCommand := ApplyMsg{
		CommandIndex : rf.snapshottedIndex,
		Command: "InstallSnapshot",
		CommandValid : false,
		CommandData : rf.persister.ReadSnapshot(),
	}

	go func(msg ApplyMsg){
		rf.applyCh <- msg
	}(installsnapshotCommand)
}






