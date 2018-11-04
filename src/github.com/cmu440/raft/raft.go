//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = Make(...)
//   Create a new Raft peer.
//
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (e.g. tester) on the
//   same peer, via the applyCh channel passed to Make()
//

import "sync"
import "github.com/cmu440/rpc"
import "fmt"
import "time"
import "math/rand"


//
// ApplyMsg
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same peer, via the applyCh passed to Make()
//
type  logEntry struct{
	command interface{}
	term int
}
type ApplyMsg struct {
	Index   int
	Command interface{}
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	currentTerm int
	//0 = leader, 1 = follower, 2 = candidate
	currentState int
	votedFor int
	log []logEntry // need this or no?
	//need a timer here?

}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	//rf.mux.Lock()
	//need to use lock here??
	var me int
	var term int
	var isleader bool
	// Your code here (2A)
	//rf.mux.Lock()
	//defer rf.mux.Unlock()
	me = rf.me
	term = rf.currentTerm
	if(rf.currentState == 0){
		isleader = true
	} else{
		isleader = false
	}
	
	return me, term, isleader
}

type AppendEntriesArgs struct{
	Term int
	LeaderId int
	//Entries []*logEntry


}

type AppendEntriesReply struct{
	Term int 
	Success bool
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	// Your data here (2A, 2B)
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	// Your data here (2A)
	Term int
	VoteGranted bool
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)

	//need to lock here?
	//
	//fmt.Printf("request vote called\n")
	//fmt.Printf("%d %d\n", args.Term, rf.currentTerm)
	rf.mux.Lock()
	if (args.Term < rf.currentTerm){
		//fmt.Printf("going into this case 1\n")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	} else if (rf.currentTerm < args.Term){
		//fmt.Printf("going into this case 2\n")
		reply.Term = rf.currentTerm
		rf.currentState = 1
		rf.currentTerm = args.Term
		rf.votedFor = -1
		
		go rf.resetTimeout()
		//reset election timer??
	}

	//fmt.Printf("the raft value is %d voted for is %d and cand id is %d\n", rf.me, rf.votedFor, args.CandidateId)
	if (rf.votedFor == -1){
		//fmt.Printf("going into this case 3\n")
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
	} else if (rf.votedFor  == args.CandidateId){
		//fmt.Printf("going into this case 4\n")
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
	}

	if(rf.currentState == 1 && reply.VoteGranted == true){
		//fmt.Printf("going into this case 2\n")
		go rf.resetTimeout()
	}
	rf.mux.Unlock()
	return
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	//need to lock here?
	rf.mux.Lock()
	fmt.Printf("AppendEntries is called\n")
	if(args.Term < rf.currentTerm){
		reply.Term = rf.currentTerm
		reply.Success = false
		
	}else if(args.Term == rf.currentTerm){
		//if follower then resetelection 
			//then candidate
		//if candidate then follower
		if (rf.currentState == 1){
			fmt.Printf("follower reseting the timeout")
			go rf.resetTimeout()

		}
		if (rf.currentState == 2){
			fmt.Printf("candidate reseting the timeout")
			reply.Term = rf.currentTerm
			rf.currentState = 1
			rf.currentTerm = args.Term
			rf.votedFor = -1
			go rf.resetTimeout()
		}
	}else{
		reply.Term = rf.currentTerm
		rf.currentState = 1
		rf.currentTerm = args.Term
		rf.votedFor = -1		
	}
	rf.mux.Unlock()
	return
	// deal with heartbeat here? 
}
//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a peer
//
// peer int -- index of the target peer in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which peers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead peer, a live peer that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the peer side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// Start
// =====
//
// The service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this peer is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this peer believes it is
// the leader
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B)

	return index, term, isLeader
}

//
// Kill
// ====
//
// The tester calls Kill() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Kill() {
	// Your code here, if desired
}

//
// Make
// ====
//
// The service or tester wants to create a Raft peer
//
// The port numbers of all the Raft peers (including this one)
// are in peers[]
//
// This peer's port is peers[me]
//
// All the peers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages
//
// Make() must return quickly, so it should start Goroutines
// for any long-running work
//

// when to send heartbeat???
func (rf *Raft) sendHeartBeat (i int, a *AppendEntriesArgs, r *AppendEntriesReply){
	result := rf.sendAppendEntries(i, a, r)
	//fmt.Printf("the result is %b\n",result)
	if (result == true){
		//lock here?
			rf.mux.Lock()
		if (r.Term > rf.currentTerm){
			fmt.Printf("In this case")
			rf.currentTerm = r.Term
			rf.currentState = 1
			rf.votedFor = -1
			rf.mux.Unlock()
		}
		rf.mux.Unlock()
	}

}

func (rf *Raft)sendVR(i int, a *RequestVoteArgs, r* RequestVoteReply, c chan bool){
	//fmt.Printf("Send vote requeset to id number %d from raft number %d\n", i, rf.me)
	result := rf.sendRequestVote(i, a, r)
	//fmt.Printf("the result is %t\n",result)
	if (result == true){
		//fmt.Printf("voteGranted is %t\n", r.VoteGranted)
		//lock here?
		if (r.VoteGranted == true){
			//fmt.Printf("sent true on channel to raft %d from id %d\n",rf.me, i )
			c<- true

		}else{
			if (r.Term > rf.currentTerm){
				rf.mux.Lock()
				rf.currentTerm = r.Term
				rf.currentState = 1
				rf.votedFor = -1
				rf.mux.Unlock()
			}
			c <- false
		}
	}

}
func (rf *Raft) heartBeat(){
    for{
    	if(rf.currentState == 0){
    		for i,_ := range(rf.peers){
				if (i != rf.me){
					heartBeatArgs := &AppendEntriesArgs{Term:rf.currentTerm,
														LeaderId:rf.me,
														}
					heartBeatReply := &AppendEntriesReply{}
					fmt.Printf("sending heartbeat from %d to %d\n", rf.me, i)
					go rf.sendHeartBeat(i, heartBeatArgs, heartBeatReply)
				//need to also check the term? 				
				}
			}					
		} else{
			return
		}
		
		time.Sleep(200 * time.Millisecond)
	}
}




func (rf *Raft) analyzeVotes(c chan bool, t int){
	totalPeers := len(rf.peers)
	count := 0
	for elem := range(c){
		rf.mux.Lock()
		// check if the term is the same by locking?
		if (elem == true){
			//fmt.Printf("inside the elem is true case for the id %d\n", rf.me)
			count += 1 
		}
		if (t != rf.currentTerm){
			rf.mux.Unlock()
			return
		}
		if(rf.currentState != 2){
			rf.mux.Unlock()
			return
		}
		if (count + 1 > totalPeers/2){
			//fmt.Printf("Became a leader %d\n", rf.me)
			if (rf.currentState == 2){
				rf.currentState = 0
				//rf.votedFor = rf.me
				go rf.heartBeat()
			}
			rf.mux.Unlock()
			return 
			//start go routine that sends heatbeats //only do this if it is still in a valid state
			// need to end this go routine at some point			
		}
		
	}
	return
}


func (rf *Raft) newElection(){
	//fmt.Printf("New election started by %d\n", rf.me)
	
	rf.currentTerm = rf.currentTerm +1

	numPeers := len(rf.peers)
	votes := make(chan bool, numPeers) // buffered channel? 
	for i,_ := range(rf.peers){
		if (i != rf.me){
			//fmt.Printf("me is %d\n",rf.me)
			//fmt.Printf("votedFor is %d\n", rf.votedFor)
		
			sendVoteArgs := &RequestVoteArgs{Term:rf.currentTerm,
											 CandidateId:rf.me}
			sendVoteReply := &RequestVoteReply{}
			go rf.sendVR(i, sendVoteArgs, sendVoteReply, votes)
		}
	}
	t := rf.currentTerm
	go rf.analyzeVotes(votes, t)	
}

func (rf *Raft) resetTimeout(){
	// should this be for select?
	fmt.Printf("resetTimeout is called %d %d\n", rf.me, rf.currentState )
	l := rand.Intn(750) + 400
	timer := time.NewTimer(time.Duration(l) * time.Millisecond)
	rf.mux.Lock()
	t := rf.currentTerm
	rf.mux.Unlock()
	for{
		select {
	  		case <-timer.C:
	  			rf.mux.Lock()
	  			if(t != rf.currentTerm){
	  				rf.mux.Unlock()
	  				return
	  			}
	  			if (rf.currentState == 1){
	  				fmt.Printf("Becomes a candidate and starts election %d\n", rf.me)
					rf.currentState = 2
					//fmt.Printf("the current votedFor is %d\n", rf.votedFor)
					rf.votedFor = rf.me
					rf.newElection()
			//need to set the votedFor to candidate id? 
			//count yourself 
				} else if (rf.currentState == 2){
					fmt.Printf("Already a candidate and starts election %d\n", rf.me)
					rf.newElection()
				}
				rf.mux.Unlock()
	  			//lock here and check that the term is the same 

	    	// timeout
		}
  		
	}
	//what to do here?

}

func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	//fmt.Print("Make called");
	rf := &Raft{}
	rf.mux.Lock()
	rf.peers = peers
	rf.me = me
	//fmt.Printf("Make called %d\n", rf.me);
	//what is supposed to happen in make? 
	//lock here??
	

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.currentState = 1 //does raft start off as a follower

	//what is supposed to happen in make? 
	go rf.resetTimeout()
	
	rf.mux.Unlock()
	// Your initialization code here (2A, 2B)

	return rf
}
