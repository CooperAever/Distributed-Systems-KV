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

// import "bytes"
// import "labgob"



// const state
const Follower  = 1
const Candidate  = 2
const Leader  = 3

// const time setup
const heartbeatInterval = time.Duration(120) * time.Millisecond
const electionTimeoutLower = time.Duration(300) * time.Millisecond
const electionTimeoutUpper = time.Duration(400) * time.Millisecond


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}


type LogEntry struct{
	Term int
	Command      interface{}
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
	currentTerm int  //2A
	voteFor int //2A
	state int //2A
	heartbeatTimer *time.Timer //2A
	electionTimer *time.Timer //2A

	log []LogEntry 		// 2B : first index is 1
	commitIndex int 	// 2B : index of highest log entry known to be committed
	lastApplied int 	// 2B : index of highest log entry applied to state machine
	// volatile state on leaders, Reinitialized after election
	nextIndex []int 	// 2B : for each server,index of the next log entry to send to that server
	matchIndex []int	// 2B : for each server,index of highest log entry known to be replicated on server
	applyCh chan ApplyMsg
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term int 				//2A candidate's term
	CandidateId int 		//2A candidate requesting vote

	LastLogIndex int  		//2B index of candidate's last log entry
	LastLogTerm int  		//2B term of candidate's last log entry

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 				//2A currentTerm,for candidate to update itself
	VoteGranted bool 		//2A true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
	}


	// Your code here (2B).
	// candidate's log is at least as up-to-date as receiver's log,grant vote
	lastLogIndex := len(rf.log)-1
	if args.LastLogTerm < rf.log[lastLogIndex].Term || 
		(args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < lastLogIndex){
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.voteFor = args.CandidateId

	// Reset time after response
	rf.electionTimer.Reset(randTimerDuration(electionTimeoutLower,electionTimeoutUpper))
	return

}




//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).

	Term int 			//2A Leader's Term
	LeaderId int 		//2A so follower can redirect clients

	PrevLogIndex int 	//2B index of log entry immediately preceding new ones
	PrevLogTerm int 	//2B term of prevLogIndex entry
	Entries []LogEntry	//2B log entries to store(empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int 	//2B Leader's commit Index


}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term int 			//2A current Term,for leader to update itself
	Success bool 		//2A true if follower contained entry matching prevLogIndex and prevLogTerm

	//optimize: with two info following, the leader can 
	//decrement nextIndex to bypass all of the conflicting
	// entries in that term.
	// one AppendEntries RPC will be required for each term with conflicting entries,
	// rather than one RPC per entry.
	ConflictEntry int 	//2B the first index stores for that term
	ConflictTerm int 	//2B term of the conflicting entry
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
	}

	// reset election timer even log does not match
	// args.LeaderId is the current term's Leader
	rf.electionTimer.Reset(randTimerDuration(electionTimeoutLower, electionTimeoutUpper))
	

	// entries before prevLogIndex may not match
	// return false to tell Leader decrement prevLogIndex
	if len(rf.log) < args.PrevLogIndex+1 {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictEntry = len(rf.log)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex
		for rf.log[conflictIndex-1].Term == reply.ConflictTerm{
			conflictIndex--
		}
		reply.ConflictEntry = conflictIndex
		return
	}

	unmatch_index := -1
	for idx := range args.Entries {
		if len(rf.log) < args.PrevLogIndex + idx + 2 || 
			rf.log[args.PrevLogIndex+idx+1].Term != args.Entries[idx].Term{

			unmatch_index = idx
			break
		}
	}

	if unmatch_index != -1{
		rf.log = rf.log[:args.PrevLogIndex+unmatch_index+1]
		rf.log = append(rf.log,args.Entries[unmatch_index:]...)
	}

	// Leader tell Follower which part of entries have been committed
	if args.LeaderCommit > rf.commitIndex{
		lastLogIndex := len(rf.log)-1
		if args.LeaderCommit > lastLogIndex{
			rf.setCommitIndex(args.LeaderCommit)
		}else{
			rf.setCommitIndex(lastLogIndex)
		}
	}


	// find the first unmatch index
	reply.Success = true
	return
}


// when Leader update rf's commitIndex , rf immediately
// apply command to state machine through ApplyMsg
func (rf *Raft) setCommitIndex(commitIndex int){
	rf.commitIndex = commitIndex

	if rf.commitIndex > rf.lastApplied{
		go func(start int, entries []LogEntry){
			for idx,entry := range entries{
				msg := ApplyMsg{
					CommandValid: true ,
					Command: entry.Command,
					CommandIndex: start + idx,
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied = start+idx
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1,rf.log[rf.lastApplied+1:rf.commitIndex+1])
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


// should be called with a lock
func (rf *Raft) convertTo(s int){
	if rf.state == s{
		return
	}
	rf.state = s

	switch s{

	case Follower:
		rf.voteFor = -1
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimerDuration(electionTimeoutLower,electionTimeoutUpper))
	
	case Candidate:
		rf.startElection()

	case Leader:
		// fmt.Printf("server %d become Leader, term %d\n",rf.me,rf.currentTerm)
		rf.electionTimer.Stop()
		rf.broadcastHeartbeat()
		rf.heartbeatTimer.Reset(heartbeatInterval)
	}

}

// one Candidate begin to send voteRequest and get elected
// should be call with a lock
func (rf *Raft) startElection(){

	rf.currentTerm += 1
	rf.electionTimer.Reset(randTimerDuration(electionTimeoutLower,electionTimeoutUpper))
	// fmt.Printf("server %d start election, term %d\n",rf.me,rf.currentTerm)
	args := RequestVoteArgs{
		Term : rf.currentTerm,
		CandidateId : rf.me,
		LastLogIndex: len(rf.log)-1,
		LastLogTerm : rf.log[len(rf.log)-1].Term,
	}
	voteCount := 0
	
	for i := range rf.peers{
		if i== rf.me {
			voteCount+=1
			rf.voteFor = rf.me
			continue
		}

		go func(server int){
			var reply RequestVoteReply
			if rf.sendRequestVote(server,&args,&reply){
				rf.mu.Lock()
				if reply.VoteGranted && rf.state == Candidate {
					voteCount+=1
					// fmt.Printf("%d getting one vote,sum : %d\n",rf.me,voteCount)
					if(voteCount > len(rf.peers)/2){
						rf.convertTo(Leader)
					}
				}else{
					if reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}

}

func (rf *Raft) broadcastHeartbeat(){

	for i:= range rf.peers{
		if i == rf.me{
			continue
		}

		go func(server int){
			rf.mu.Lock()
			if rf.state != Leader{
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[server]-1

			entries := make([]LogEntry,len(rf.log[prevLogIndex+1:]))
			copy(entries,rf.log[prevLogIndex+1:])

			args:= AppendEntriesArgs{
				Term:rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm : rf.log[prevLogIndex].Term,
				Entries : entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply

			if rf.sendAppendEntries(server,&args,&reply){
				rf.mu.Lock()
				if reply.Success{
					rf.matchIndex[server] = args.PrevLogIndex+len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server]+1

					for i:=len(rf.log)-1;i>rf.commitIndex;i--{
						count :=0
						for _,matchIndex := range rf.matchIndex{
							if matchIndex>=i {
								count += 1
							}
						}

						if(count > len(rf.peers)/2){
							rf.setCommitIndex(i)
							break
						}
					}
				}else{
					if reply.Term > args.Term{
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
					}else{
						rf.nextIndex[server] = reply.ConflictEntry

						if reply.ConflictEntry != -1{
							for i:= args.PrevLogIndex ; i>=1 ;i--{
								if rf.log[i-1].Term == reply.ConflictTerm{
									rf.nextIndex[server] = i
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
		index = len(rf.log)
		rf.log = append(rf.log,LogEntry{Term:term,Command:command})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index+1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower //2A
	rf.currentTerm = 0  //2A
	rf.voteFor = -1		//2A
	rf.heartbeatTimer = time.NewTimer(heartbeatInterval)
	rf.electionTimer = time.NewTimer(randTimerDuration(electionTimeoutLower,electionTimeoutUpper))
	rf.applyCh = applyCh
	rf.log = make([]LogEntry,1) //2B,start from index 1
	rf.nextIndex = make([]int,len(rf.peers))
	for i:= range rf.nextIndex{
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int,len(rf.peers))

	fmt.Printf("creating server %d term %d \n ",rf.me,rf.currentTerm)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(rf *Raft){
		for{
			select{
			case <- rf.electionTimer.C:
				rf.mu.Lock()
				if rf.state == Follower{
					rf.convertTo(Candidate)
				}else{
					rf.startElection()
				}
				rf.mu.Unlock()

			case <- rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == Leader{
					rf.broadcastHeartbeat()
					rf.heartbeatTimer.Reset(heartbeatInterval)
				}
				rf.mu.Unlock()
			}
		}
	}(rf)

	return rf
}

func randTimerDuration(electionTimeoutLower,electionTimeoutUpper time.Duration) time.Duration{
	num := rand.Int63n(electionTimeoutUpper.Nanoseconds() - electionTimeoutLower.Nanoseconds()) + electionTimeoutLower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}
