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
	"labgob"
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"time"
)

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

//
// A Go object implementing a single Raft peer.
//

// Each log entry will be of following struct
type LogEntry struct {
	Term  int
	Key   int
	Value int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// My 2A
	// Persistent states
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile states
	commitIndex int
	lastApplied int

	// Current role - considering this as persistent (doubt)
	// 0 Follower, 1 Candidate, 2 Leader
	currentRole int

	// Election timeout random value
	electionOffset  time.Time
	heartBeatOffset time.Time

	voteCnt      int
	outdatedTerm bool
	newTerm      int

	isKilled bool
	applyCh  chan ApplyMsg

	//Leaders state: Only valid if rf.currentRole = 2
	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//
func (rf *Raft) InitializeLeader() bool {
	rf.mu.Lock()
	rf.currentRole = 2
	rf.votedFor = rf.me
	rf.persist()

	serverCnt := len(rf.peers)
	rf.nextIndex = make([]int, serverCnt)
	rf.matchIndex = make([]int, serverCnt)
	for i := 0; i < serverCnt; i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = -1
	}

	rf.mu.Unlock()
	// Send initial empty append Entries to all server
	// If inconsistency is detected update leader's log
	rf.checkConsistency()
	return true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.currentRole == 2 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}
func (rf *Raft) GetRandomTime() int {

	// initiating the timer
	// Resoning behind taking timeout between 275-425 ms:
	// 		Given we have 10 hearbeat per 100 ms, so by the 275 ms we can expect atleast 2 heart beats
	// 		and max server will wait for 425 ms, i.e wait time of atleast 4 hearbeats
	minTimer := 300
	maxTimer := 500
	result := (minTimer + rand.Intn(maxTimer-minTimer))
	return result

}
func (rf *Raft) convertToFollower(term int) {
	rf.mu.Lock()
	rf.currentRole = 0
	// rf.electionOffset = time.Now()
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
	rf.mu.Unlock()

}
func (rf *Raft) VotingHandler(i int, majority_cnt int, args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("385 server %v is sending vote RPC request to %v", rf.me, i)
	// if i == rf.me {
	// 	return
	// }
	// responded := rf.sendRequestVote(i, args, reply)
	// if !responded {
	// 	return
	// }
	//DPrintf("385 Vote RPC Response by server %v: %v", i, responded)
	rf.mu.Lock()
	if rf.voteCnt < majority_cnt {
		DPrintf("435 %v, %v, %v", reply.Term == rf.currentTerm, reply.Term == rf.currentTerm, reply.VoteGranted == true)
		DPrintf("435 replyTerm %v, Current Term, %v", reply.Term, rf.currentTerm)
		if reply.Term == rf.currentTerm && reply.VoteGranted == true && rf.currentRole == 1 {
			DPrintf("388 %d recieved vote for term %d by server %v", rf.me, rf.currentTerm, i)
			rf.voteCnt++
			if rf.voteCnt >= majority_cnt {
				//be leader
				DPrintf("Election won for term %d by: %d with vote cnt %d for term %d", rf.currentTerm, rf.me, rf.voteCnt, rf.currentTerm)
				rf.mu.Unlock()
				rf.InitializeLeader()
				rf.mu.Lock()
			}
		} else if reply.Term > rf.currentTerm {
			//time to convert back to follower
			DPrintf("Time to convert back to follower...outdated term")
			DPrintf("389 Updating current term of server %d from server %d", rf.me, args.CandidateId)
			DPrintf("From %d to %d", rf.currentTerm, reply.Term)
			rf.mu.Unlock()
			rf.convertToFollower(reply.Term)
			rf.mu.Lock()
		}
	}
	rf.mu.Unlock()

}

func (rf *Raft) ElectionTimer() {
	for {
		rf.mu.Lock()
		if rf.isKilled {
			rf.mu.Unlock()
			break
		}
		randomTime := rf.GetRandomTime()
		t1 := rf.electionOffset
		currRole := rf.currentRole
		rf.mu.Unlock()
		duration := time.Since(t1)
		if duration.Seconds()*1e3 > float64(randomTime) && currRole <= 1 {

			rf.mu.Lock()
			DPrintf("144 Timeout by server %d...Converting to candidate with log %+v", rf.me, rf.logs)
			DPrintf("145 Time out value was: %v", randomTime)
			rf.currentTerm++
			rf.currentRole = 1
			rf.voteCnt = 1
			rf.electionOffset = time.Now()
			rf.votedFor = rf.me
			rf.persist()

			DPrintf("Election initialed by: %d for term: %d", rf.me, rf.currentTerm)

			rf.mu.Unlock()
			server_cnt := len(rf.peers)
			majority_cnt := int(math.Ceil(float64(server_cnt) / float64(2)))

			// var wg sync.WaitGroup
			for i := 0; i < server_cnt; i++ {
				go func(x int) {
					rf.mu.Lock()
					if x == rf.me {
						rf.mu.Unlock()
						return
					}
					args := &RequestVoteArgs{}
					reply := &RequestVoteReply{}
					args.Term = rf.currentTerm //incrementing term
					args.CandidateId = rf.me
					args.LastLogIndex = len(rf.logs)
					if args.LastLogIndex == 0 {

						args.LastLogTerm = rf.currentTerm
					} else {
						args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}
					rf.mu.Unlock()
					responded := rf.sendRequestVote(x, args, reply)
					if !responded {
						return
					} else {
						rf.VotingHandler(x, majority_cnt, args, reply)
					}

					// rf.VotingHandler(x, majority_cnt, args, reply, responded)
				}(i)
				// go rf.VotingHandler(i, majority_cnt, args, reply)
			}
		}
		time.Sleep(10 * time.Millisecond)

	}
}

func (rf *Raft) SendHeartbeats() {
	// //DPrintf("Timer started parallely")
	// trigger if duration is greater than duration
	for {
		rf.mu.Lock()
		if rf.isKilled {
			rf.mu.Unlock()
			break
		}
		HearBeatTime := 100
		t1 := rf.heartBeatOffset
		currRole := rf.currentRole
		rf.mu.Unlock()
		duration := time.Since(t1)
		if duration.Seconds()*1e3 > float64(HearBeatTime) && currRole == 2 {
			DPrintf("Leader server %d is sending heartbeats via sendHeartBeats()", rf.me)
			rf.checkConsistency()
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("Error..no values found")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int

	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler
//
func (rf *Raft) FollowerVotingLogic(args *RequestVoteArgs, reply *RequestVoteReply) {
	// rf.mu.Lock()
	// // rf.electionOffset = time.Now()
	// rf.mu.Unlock()
	rf.mu.Lock()
	currLastTerm := rf.currentTerm
	if len(rf.logs) > 0 {
		currLastTerm = rf.logs[len(rf.logs)-1].Term
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > currLastTerm || (args.LastLogTerm == currLastTerm && (args.LastLogIndex) >= len(rf.logs))) {
		DPrintf("306 follower: server %d is granting vote to: %d", rf.me, args.CandidateId)
		DPrintf("308 Server %v updating votedfor from %d to %d", rf.me, rf.votedFor, args.CandidateId)

		rf.votedFor = args.CandidateId
		rf.electionOffset = time.Now()
		rf.persist()

		reply.VoteGranted = true
		reply.Term = rf.currentTerm

	} else {

		DPrintf("317 Rejected vote by server %d for server %d", rf.me, args.CandidateId)
		DPrintf("rf.votedFor %d", rf.votedFor)
		DPrintf("args.LastLogTerm > rf.currentTerm %d > %d", args.LastLogTerm, rf.currentTerm)
		DPrintf("args.LastLogIndex >= len(rf.logs)-1 %d > %d", args.LastLogIndex, len(rf.logs)-1)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm //doubt what to do
	}
	rf.mu.Unlock()
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.isKilled {
		rf.mu.Unlock()
		return
	}
	DPrintf("Request vote recieved by server %d with role: %d from server %d", rf.me, rf.currentRole, args.CandidateId)
	if rf.currentRole != 1 {
		// folower...only respond to RPC and term update
		// To doReset its clock
		// rf.electionOffset = time.Now()
		if rf.currentTerm > args.Term {
			DPrintf("296 Rejected vote by server %d for server %d", rf.me, args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm

		} else if rf.currentTerm == args.Term {
			rf.mu.Unlock()
			rf.FollowerVotingLogic(args, reply)
			rf.mu.Lock()

		} else {
			//term mismatch...higher term is there, as a follower i have to update it
			DPrintf(" 300 Updating current term of %d from server %d", rf.me, args.CandidateId)
			DPrintf("From %d to %d", rf.currentTerm, args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			rf.FollowerVotingLogic(args, reply)
			rf.mu.Lock()
		}
	} else if rf.currentRole == 1 {
		// other candidate is asking for vote i.e i have to respond now
		// check rpc
		// 1. if my term < asking term step down to follower... outdated terms tracked and respond like a follower
		if rf.currentTerm < args.Term {
			// Go back being a follower

			DPrintf("327 Updating current term of %d from server %d", rf.me, args.CandidateId)
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
			rf.currentRole = 0
			rf.mu.Unlock()
			rf.FollowerVotingLogic(args, reply)
			rf.mu.Lock()
		} else if rf.currentTerm == args.Term {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm

		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm

		}
	} else {
		// implies role is leader... only respond
		if rf.currentTerm > args.Term {
			DPrintf("484 Rejected vote by server %d for server %d", rf.me, args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm

		} else if rf.currentTerm == args.Term {
			DPrintf("488 Rejected vote by server %d for server %d", rf.me, args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm

		} else {
			// leader found a outdated term via rpc from other candidate
			// to do: step down as leader
			DPrintf("495 Updating current term of server %d from server %d", rf.me, args.CandidateId)
			DPrintf("From rf.curr = %d and argsTerm = %d", rf.currentTerm, args.Term)
			rf.currentRole = 0
			rf.currentTerm = args.Term
			rf.votedFor = -1

			rf.persist()
			// rf.electionOffset = time.Now()

			// Behave like a follower
			rf.mu.Unlock()
			rf.FollowerVotingLogic(args, reply)
			rf.mu.Lock()
		}
	}
	rf.mu.Unlock()
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
func (rf *Raft) checkAllSame(start int, entries []LogEntry) bool {
	for i := 0; i < len(entries); i++ {
		if rf.logs[start+i] != entries[i] {
			return false
		}
	}
	return true
}
func (rf *Raft) processRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("server %d logs %+v", rf.me, rf.logs)
	DPrintf("server %d is processing appendRPC %+v", rf.me, args)
	// 2.
	if args.PrevLogTerm != -1 && (len(rf.logs) < args.PrevLogIndex+1 || (rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm)) {
		reply.Success = false
		reply.Term = args.Term
		DPrintf("server %d rejected RPC in stage 2 (logs dont agree)", rf.me)
		return
	}
	// So here this server have agreed with the prevLog term of leader

	// Checking if the entry already in logs
	if len(args.Entries) == 0 {
		DPrintf("server %d recieved empty heartbeat... already updated rf.logs: %+v", rf.me, rf.logs)
		reply.Success = true
		reply.Term = rf.currentTerm
		DPrintf("server %d is sending reply.Success: %v", rf.me, reply.Success)
	} else {
		// 3 "If" an existing entry at args.PrevLogIndex, we will delete it and all that follows it
		if len(rf.logs) > 1 && len(rf.logs) > (args.PrevLogIndex+1) && rf.logs[args.PrevLogIndex+1].Term != args.Entries[0].Term {
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			DPrintf("conflicting entris found...server %d in stage 3", rf.me)
		}
		// 4. Append any new entry not already in the log
		DPrintf("Server %d Insering new entry args.prevLogIndex:%d", rf.me, args.PrevLogIndex)
		DPrintf("Before Inserting logs: %+v", rf.logs)
		for i := 0; i < len(args.Entries); i++ {
			newIdx := args.PrevLogIndex + 1 + i
			if newIdx >= len(rf.logs) {
				temp := args.Entries[i]
				rf.logs = append(rf.logs, temp)
			} else {
				rf.logs[args.PrevLogIndex+i+1] = args.Entries[i]
			}
		}
		rf.persist()
		reply.Success = true
		reply.Term = rf.currentTerm
		DPrintf("Server %d After Insertion of new entries logs: %+v", rf.me, rf.logs)

	}
	// 5. Check Leader commit
	if args.LeaderCommit > rf.commitIndex {
		// rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.lastNewEntryIndex)))
		// rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// Trigger the ApplyCh
		DPrintf("Follower updated commitIndex via rpc to: %v", rf.commitIndex)

	}
	// So here this server have agreed with the prevLog term of leader
	if rf.lastApplied < rf.commitIndex {
		rf.mu.Unlock()
		go rf.sendToApplyCh()
		rf.mu.Lock()
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	if rf.isKilled {
		rf.mu.Unlock()
		return
	}
	DPrintf("Server %v is in term %d recieved Heartbeat from Leader: %v for term: %v", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	} else {
		DPrintf("545 Updating current term of server %d from server %d", rf.me, args.LeaderId)
		DPrintf("From %d to %d", rf.currentTerm, args.Term)
		rf.votedFor = -1
		rf.currentRole = 0
		rf.currentTerm = args.Term

		rf.persist()
		rf.electionOffset = time.Now()
		// if rf.currentTerm <= args.Term {
		// That means rf.currentTerm <= args.Term.. we can process rpc
		rf.processRPC(args, reply)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
func (rf *Raft) informServer(x int) bool {
	rf.mu.Lock()
	// intended nextIndex according to leader
	DPrintf("leader server %d is contacting server %d to update logs via rf.informServer", rf.me, x)
	DPrintf("leader server %d has logs: %+v", rf.me, rf.logs)
	//Preparing the args
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[x] - 1
	if args.PrevLogIndex < 0 {
		// Logs are completely empty
		args.PrevLogTerm = -1
		//Args.Entries will be empty
	} else if args.PrevLogIndex >= len(rf.logs) {
		args.PrevLogIndex = len(rf.logs) - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.Entries = rf.logs[args.PrevLogIndex+1:]
	} else {
		// Normal Case
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.Entries = rf.logs[args.PrevLogIndex+1:]
	}

	DPrintf("consistencycheck...sending initial args: %+v", args)
	args.LeaderCommit = rf.commitIndex
	DPrintf("Sending rpc req to server %d ", x)
	rf.mu.Unlock()
	response := rf.sendAppendEntries(x, args, reply)
	rf.mu.Lock()
	flag := false
	DPrintf("Got rpc response for server %d %v , reply.success: %v, reply.Term %d", x, response, reply.Success, reply.Term)
	if response {
		if reply.Term < rf.currentTerm {
			rf.currentRole = 0
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			DPrintf(" server %d has larger term in system...sever %d backing off as leader", x, rf.me)
		} else {
			// Implies log don't agree...decrement and resend
			for !reply.Success && rf.currentRole == 2 && flag == false {
				if reply.Term > rf.currentTerm {
					DPrintf(" server %d has larger term in system...sever %d backing off as leader", x, rf.me)
					//Converting to follower
					rf.currentRole = 0
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
				} else {
					DPrintf("Log mismatch for server %d decrementing  %+v , reply.success: %v", x, response, reply.Success)
					rf.nextIndex[x]--
					args := &AppendEntriesArgs{}
					reply := &AppendEntriesReply{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PrevLogIndex = rf.nextIndex[x] - 1
					if args.PrevLogIndex < 0 {
						// Logs are completely empty
						args.PrevLogTerm = -1
						//Args.Entries will be empty
					} else if args.PrevLogIndex >= len(rf.logs) {
						args.PrevLogIndex = len(rf.logs) - 1
						args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
						args.Entries = rf.logs[args.PrevLogIndex+1:]
					} else {
						// Normal Case
						args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
						args.Entries = rf.logs[args.PrevLogIndex+1:]
					}
					DPrintf("consistencycheck...sending updated args: %+v", args)
					rf.mu.Unlock()
					response = rf.sendAppendEntries(x, args, reply)
					rf.mu.Lock()
					DPrintf("AppendRPC response from server %d, reply.success: %v", x, reply.Success)
					if reply.Success {
						flag = true
					}

				}
			}
		}
		if reply.Success && rf.currentRole == 2 {
			rf.matchIndex[x] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[x] = rf.matchIndex[x] + 1
			DPrintf("server %d has agreed on logs from leader server%d", x, rf.me)
			DPrintf("Updating matchIndex[%d] to %d", x, rf.matchIndex[x])
			rf.mu.Unlock()
			return true
		}
		if rf.currentRole != 2 {
			rf.mu.Unlock()
			DPrintf("Leader %d just transformed to follower...", rf.me)
			return false
		}

	}
	rf.mu.Unlock()
	return false
}

func (rf *Raft) updateCommitIndex() bool {
	DPrintf("Server %d appended on majority..calculating new commitIndex", rf.me)
	DPrintf("Before update rf.commitIndex is %d and len(rf.logs) is %d", rf.commitIndex, rf.logs)
	newCommitIndex := rf.commitIndex
	majorityCnt := int(math.Ceil(float64(len(rf.peers)) / float64(2)))
	for i := rf.commitIndex + 1; i <= len(rf.logs); i++ {
		DPrintf("Checking with N: %d", i)
		cnt := 1
		for j := 0; j < len(rf.nextIndex); j++ {
			if rf.matchIndex[j] >= i && j != rf.me {
				cnt++
			}
		}
		if cnt >= majorityCnt && rf.logs[i].Term == rf.currentTerm {
			DPrintf("N: %d for commit index %d and majority cnt: %d ", cnt, i, majorityCnt)
			DPrintf("rf.logs[i].Term %d and  rf.currentTerm %d", rf.logs[i].Term, rf.currentTerm)
			newCommitIndex = i

		}
	}
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		DPrintf("Updating commitIndex: %d ", rf.commitIndex)
		DPrintf("Triggering sendToApplyCh")
		rf.mu.Unlock()
		go rf.sendToApplyCh()
		rf.mu.Lock()
		return true
	}
	return false
}

func (rf *Raft) checkConsistency() {
	rf.mu.Lock()
	if rf.isKilled {
		rf.mu.Unlock()
		return
	}
	rf.heartBeatOffset = time.Now()
	serverCnt := len(rf.peers)
	positiveResponse := 1 // counting own appended entry
	majorityCnt := int(math.Ceil(float64(serverCnt) / float64(2)))
	DPrintf("Majority cnt is : %d", majorityCnt)
	// triggerCommitProcess := false
	for i := 0; i < serverCnt && rf.currentRole == 2; i++ {
		rf.mu.Unlock()
		go func(x int) {
			rf.mu.Lock()
			if x == rf.me {
				// if the server is itself, it will update match index to its own logs
				rf.matchIndex[x] = len(rf.logs)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			done := rf.informServer(x)
			rf.mu.Lock()
			if done && rf.currentRole == 2 {
				DPrintf("Recieved success from server %d cmd", x)
				positiveResponse++
				DPrintf("positiveResponses so far: %d", positiveResponse)
			}
			if positiveResponse >= majorityCnt {
				DPrintf("Going for commit index update since positiveResponses so far: %d", positiveResponse)
				// triggerCommitProcess = true
				rf.updateCommitIndex()
			}
			rf.mu.Unlock()
			return
		}(i)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()
	if !isLeader {
		DPrintf("Start call to a follower server %d..Returning the start function ith index...%d", rf.me, index)
		return index, term, isLeader
	}
	// If code comes here...that implies this server is leader and should start processing the client's request
	rf.mu.Lock()
	entry := LogEntry{}
	entry.Term = rf.currentTerm
	entry.Key = command.(int)
	rf.logs = append(rf.logs, entry)
	rf.persist()
	index = len(rf.logs) - 1
	DPrintf("START called server %d starting to append logs..leader of term %d, cmd:%d", rf.me, rf.currentTerm, entry.Key)
	rf.mu.Unlock()
	go rf.checkConsistency()
	DPrintf("Returning the start function ith index...%d", index)
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
	rf.mu.Lock()
	rf.isKilled = true
	rf.mu.Unlock()

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
func (rf *Raft) sendToApplyCh() {
	rf.mu.Lock()
	DPrintf("server %d In apply channel. Commit index:%d", rf.me, rf.commitIndex)
	for rf.commitIndex > rf.lastApplied {
		msg := ApplyMsg{}
		rf.lastApplied++
		msg.CommandValid = true
		msg.Command = rf.logs[rf.lastApplied].Key
		msg.CommandIndex = rf.lastApplied
		DPrintf("server %d Sending to ApplyCh %v", rf.me, msg)
		rf.applyCh <- msg
		DPrintf("server %d Done Sending...", rf.me)
	}
	rf.mu.Unlock()
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//DPrintf("Setting up server: %v", me)
	rf := &Raft{}
	// rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.isKilled = false

	// My 2A
	// To do: save states in persistent mode
	// Persistent states
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentRole = 0
	rf.applyCh = applyCh
	// dummy 0 entry
	dummyEntry := LogEntry{}
	dummyEntry.Term = -1
	dummyEntry.Key = 0
	rf.logs = append(rf.logs, dummyEntry)

	// Volatile states
	rf.commitIndex = 0
	rf.lastApplied = 0

	//setting up of election time out in ms
	rf.electionOffset = time.Now()
	rf.heartBeatOffset = time.Now()
	// rf.mu.Unlock()
	go rf.ElectionTimer()
	go rf.SendHeartbeats()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
