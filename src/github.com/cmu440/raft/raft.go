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
	Command interface{}
	Term int
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

	applyCh chan ApplyMsg

	commitIndex int
	lastApplied int 

	nextIndex []int 
	matchIndex []int 

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
	rf.mux.Lock()
	fmt.Printf("took lock in get state id is %d\n", rf.me)
	//defer rf.mux.Unlock()
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
	fmt.Printf("released lock in get state id is %d\n", rf.me)
	rf.mux.Unlock()
	return me, term, isleader
}

type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	
	Entries []logEntry
	LeaderCommit int


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
	LastLogIndex int
	LastLogTerm int
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

	//fmt.Printf("request vote called for CandidateId %d\n", args.CandidateId)
	//fmt.Printf("current term is %d args term is %d\n", rf.currentTerm, args.Term)
	//fmt.Printf("rf.votedFor is %d\n", rf.votedFor)
	rf.mux.Lock()
	fmt.Printf("took lock in request vote %d\n", rf.me)
	//defer rf.mux.Unlock()
	if (args.Term < rf.currentTerm){
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	} else if (rf.currentTerm < args.Term){
		reply.Term = rf.currentTerm
		rf.currentState = 1
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.VoteGranted = true		
		go rf.resetTimeout()
	}
	if (rf.votedFor == -1){

		l := len(rf.log)
		s:= l > 0 && (rf.log[l-1].Term > args.LastLogTerm || (rf.log[l-1].Term  == args.LastLogTerm && l > args.LastLogIndex))
		//need to check if its bigger than 0 
		if(!s){		
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
		} 
	}else if (rf.votedFor  == args.CandidateId){
		l := len(rf.log)
		//need to check if its bigger than 0 
		if(l > 0){
			if(rf.log[l-1].Term > args.LastLogTerm || rf.log[l-1].Term  == args.LastLogTerm && l > args.LastLogIndex){
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
			}
		}
	}
	
	if(reply.VoteGranted == true){
		go rf.resetTimeout()
	}
	fmt.Printf("released lock in request vote %d\n", rf.me)
	rf.mux.Unlock()
	return
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mux.Lock()
	fmt.Printf("took look in Append Entries id is %d\n", rf.me)
	//need to lock here?
	//defer rf.mux.Unlock()
	reply.Success = true
	if(args.Term < rf.currentTerm){
		reply.Term = rf.currentTerm
		reply.Success = false
		
	}else if(args.Term == rf.currentTerm){
		if (rf.currentState == 1){
			reply.Term = rf.currentTerm
			reply.Success = true
			//fmt.Printf("follower reseting the timeout")
			go rf.resetTimeout()

		}
		if (rf.currentState == 2){
			//fmt.Printf("candidate reseting the timeout")
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.currentState = 1
			rf.currentTerm = args.Term
			rf.votedFor = -1
			go rf.resetTimeout()
		}
	} else{
		reply.Term = rf.currentTerm
		rf.currentState = 1
		rf.currentTerm = args.Term
		rf.votedFor = -1	
		reply.Success = true	
	}

	//
	if (len(args.Entries) >0){
		check := args.PrevLogIndex> 0 && len(rf.log)  >= args.PrevLogIndex
		//fmt.Printf("The id is %d, the prev log index is %d, the len of the log is %d\n",rf.me, args.PrevLogIndex, len(rf.log))
		if (args.PrevLogIndex > 0 && (!check || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm)){
			reply.Success = false
		}
		if(!reply.Success){
			fmt.Printf("released lock in Append Entries  reply success failed id is %d\n", rf.me)
			rf.mux.Unlock()
			return
		}
	}
	//}
	//fmt.Printf("rf %v log before %+v", rf.me, rf.log)
	/* reply .success is false then return*/
	//fmt.Printf("the length of args.Entries is %d\n", len(args.Entries))

	for i, e := range args.Entries{
		if(args.PrevLogIndex + 1 + i >0 && len(rf.log) >= args.PrevLogIndex + 1 + i){
			if(rf.log[args.PrevLogIndex + i].Term != e.Term){
				rf.log = rf.log[:args.PrevLogIndex + i]
			}
			rf.log = append(rf.log, e)
		}else{
			rf.log = append(rf.log, e)
		}
	}

	if(args.LeaderCommit > rf.commitIndex){
		//check if lene args.entries is greater than 0
		if (len(args.Entries) >0 && args.LeaderCommit > args.PrevLogIndex + len(args.Entries)){
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}else{
			rf.commitIndex = args.LeaderCommit
		}
	}

	//fmt.Printf("rf %v og after %+v", rf.me, rf.log) 
	rf.applyEntries()
	go rf.resetTimeout()
	//apply entries???
	fmt.Printf("released lock in Append Entries id is %d\n", rf.me)
	rf.mux.Unlock()
	return
	// deal with heartbeat here? 
}

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
	//fmt.Printf("Start is called on id number %d\n", rf.me)
	rf.mux.Lock()
	fmt.Printf("took lock in Start id is %d\n", rf.me)
	//defer rf.mux.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := true
	if (rf.currentState != 0){
		isLeader = false
	}
	fmt.Printf("released first lock in Start id is %d\n", rf.me)
	rf.mux.Unlock()
	if (isLeader){
		//fmt.Printf("Inside is leader case in start")
		newEntry := logEntry{Command: command, 
							 Term: rf.currentTerm,}
		rf.log = append(rf.log, newEntry)
		index = len(rf.log)
		fmt.Printf("rf %v receive command %v, index is %v\n", rf.me, command, index)

		c := rf.currentTerm
		for i,_ := range(rf.peers){
			if (i != rf.me){
				rf.mux.Lock()
				fmt.Printf("took lock in Start loop id is %d\n", i)
				pLIndex := rf.nextIndex[i] -1
				pLTerm := -1
				var e []logEntry
				fmt.Printf("pLIndex is %d\n", pLIndex)
				if pLIndex > 0 && pLIndex <= len(rf.log){
					pLTerm = rf.log[pLIndex -1].Term
					e = rf.log[rf.nextIndex[i]-1:]
				}else{
					e = rf.log[:]
				}
				fmt.Printf("pLIndex is %d rf.log is %v e is %v, term is %d\n", pLIndex,rf.log,e, pLTerm)
				//check if still in leader state and term equals the current term
				//fmt.Printf("len of entries is %d\n", len(e))
				aeArgs := &AppendEntriesArgs{Term:c,
													LeaderId:rf.me,
													PrevLogIndex: pLIndex,
													PrevLogTerm:pLTerm,
													Entries: e,
													LeaderCommit:rf.commitIndex,
													}
				aeReply := &AppendEntriesReply{}
				//fmt.Printf("sending command from %d to %d\n", rf.me, i)
				rf.mux.Unlock()
				fmt.Printf("released lock in Start loop id is %d\n", i)
				rf.sendAE(i, aeArgs, aeReply)
				//fmt.Printf("released lock in Start loop id is %d\n", rf.me)
				//rf.mux.Unlock()
			//need to also check the term? 				
			}
		}					

	}

	return index, term, isLeader
}


func (rf *Raft) Kill() {
	// Your code here, if desired
}


// when to send heartbeat???
func (rf *Raft) sendAE (i int, a *AppendEntriesArgs, r *AppendEntriesReply){
	fmt.Printf("a\n")
	//fmt.Printf("sent to server : %d\n", i)
	result := rf.sendAppendEntries(i, a, r)
	fmt.Printf("id is %d result is %v r.Success is %v\n",i, result, r.Success)
	//rf.mux.Lock()
	//defer rf.mux.Unlock()
	fmt.Printf("b\n")
	if (result ){
			rf.mux.Lock()
			//defer rf.mux.Unlock()
		if(rf.currentState != 0){
			//rf.mux.Unlock()
			fmt.Printf("c\n")
			return
		}
		// if not leader then return ////////
		if (r.Term > rf.currentTerm){
			//fmt.Printf("In this case")
			rf.currentTerm = r.Term
			rf.currentState = 1
			rf.votedFor = -1
			fmt.Printf("d\n")
			//rf.mux.Unlock()
		}
		if (r.Success == true){
			fmt.Printf("e\n")
			if(len(a.Entries)> 0){
				rf.nextIndex[i] = rf.nextIndex[i] + len(a.Entries)
				rf.matchIndex[i] = rf.nextIndex[i] - 1
				//what is going on here
				for ind:= rf.commitIndex + 1; ind <= len(rf.log); ind++ {
					c := 1
					for m, j := range rf.matchIndex{
						if j >= ind && m != rf.me{
							c+= 1
						}
					}
					if (c > len(rf.peers)/2){
						if (rf.log[ind-1].Term == rf.currentTerm){
							rf.commitIndex = ind
						}
					}

			//apply entries ???
				}
				//rf.mux.Unlock()
				rf.applyEntries()
				rf.mux.Unlock()
				fmt.Printf("f\n")
				return
			}
			//rf.mux.Unlock()
		}else{
			fmt.Printf("g\n")
			if(len(a.Entries)> 0){
				fmt.Printf("h\n")
				if rf.nextIndex[i] > 1 {
					rf.nextIndex[i] = rf.nextIndex[i] - 1
					a.PrevLogIndex = rf.nextIndex[i] - 1
					a.PrevLogTerm = rf.log[a.PrevLogIndex].Term
					a.Entries = rf.log[rf.nextIndex[i]-1:]
					//a.Entries = 
					rf.mux.Unlock()
					rf.sendAE(i,a,r)
					//rf.mux.Unlock()
					//rf.mux.Unlock()
					fmt.Printf("i\n")
					return
				}
			}
			//rf.mux.Unlock()

		}
		fmt.Printf("j\n")
		rf.mux.Unlock()
		return
		//rf.mux.Unlock()
	}else{
		fmt.Printf("k\n")
		if (len(a.Entries)>0){
			//rf.mux.Unlock()
			rf.sendAE(i, a, r)
		}
		//rf.mux.Unlock()
		fmt.Printf("l\n")
		return
		
	}

}

func (rf *Raft) heartBeat(){
	fmt.Printf("heartbeat function called")
	
	rf.mux.Lock()
	c := rf.currentTerm
	//s := rf.currentState
	rf.mux.Unlock()
    for{
		for i,_ := range(rf.peers){
			fmt.Printf("entered for loop, index is %d\n", i)
			if (i != rf.me){
					//for{
						rf.mux.Lock()
						fmt.Printf("made it inside the for loop, id is %d\n",i) 
						if(rf.currentTerm != c || rf.currentState != 0){
							rf.mux.Unlock()
							return
						}
						//rf.mux.Unlock()
						//rf.mux.Lock()
						//fmt.Printf("lol\n")
						pLIndex := rf.nextIndex[i] -1
						pLTerm := -1
						var e []logEntry
						if pLIndex > 0 && pLIndex <= len(rf.log){
							pLTerm = rf.log[pLIndex -1].Term
							//e = rf.log[rf.nextIndex[i]-1:]
						}
						//check if still in leader state and term equals the current term
						heartBeatArgs := &AppendEntriesArgs{Term:c,
															LeaderId:rf.me,
															PrevLogIndex: pLIndex,
															PrevLogTerm:pLTerm,
															Entries: e,
															LeaderCommit:rf.commitIndex,
														}
						//fmt.Printf("hhhhhh\n")
						heartBeatReply := &AppendEntriesReply{}
						rf.mux.Unlock()
						//fmt.Printf("sending heartbeat from %d to %d\n", rf.me, i)
						rf.sendAE(i, heartBeatArgs, heartBeatReply)
						//rf.mux.Unlock()
						//time.Sleep(150 * time.Millisecond)
			//need to also check the term? 				
					}
				}	
			
						
		//fmt.Printf("exited the for loop")
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft)sendVR(i int, a *RequestVoteArgs, r* RequestVoteReply, c chan bool){	
	result := rf.sendRequestVote(i, a, r)	
	//fmt.Printf("the result from vote request sent to %d is %t and voteGranted is %t\n", i, result, r.VoteGranted)
	if (result){
		if (r.VoteGranted){
			//fmt.Printf("sent true on channel to raft %d from id %d\n",rf.me, i )
			c<- true

		}else{
			rf.mux.Lock()
			if (r.Term > rf.currentTerm){
				//rf.mux.Lock()
				rf.currentTerm = r.Term
				rf.currentState = 1
				rf.votedFor = -1				
			}
			rf.mux.Unlock()
			c <- false
		}
	} else{
		rf.sendVR(i,a,r,c)
	}
}


func (rf *Raft) analyzeVotes(c chan bool, t int){
	totalPeers := len(rf.peers)
	count := 0
	for elem := range(c){
		rf.mux.Lock()
		
		// check if the term is the same by locking?
		if(rf.currentTerm != t){
			return
		}
		if (elem == true){
			//fmt.Printf("inside the elem is true case for the id %d\n", rf.me)
			count += 1 
		}
		if (count + 1 > totalPeers/2){
			fmt.Printf("Became a leader %d\n", rf.me)
			if (rf.currentState == 2){
				rf.currentState = 0
				l := len(rf.log)
				for i:= range rf.peers {
					if (i != rf.me){
						rf.nextIndex[i] = l + 1
					}
				}
				//fmt.Printf("leader %v log is %+v, nextIndex is %+v\n", rf.me, rf.log, rf.nextIndex)
				//rf.votedFor = rf.me
				go rf.heartBeat()
			}
			rf.mux.Unlock()
			return 
			//start go routine that sends heatbeats //only do this if it is still in a valid state
			// need to end this go routine at some point			
		}
		rf.mux.Unlock()		
	}
	return
}


func (rf *Raft) newElection(){
	//fmt.Printf("New election started by %d\n", rf.me)
	rf.mux.Lock()
	rf.currentTerm = rf.currentTerm +1
	rf.mux.Unlock()
	numPeers := len(rf.peers)
	votes := make(chan bool, numPeers) // buffered channel? 
	for i,_ := range(rf.peers){
		if (i != rf.me){
			//fmt.Printf("me is %d\n",rf.me)
			//fmt.Printf("votedFor is %d\n", rf.votedFor)
			//fmt.Printf("send vote request to %d from %d\n", i, rf.me)
			rf.mux.Lock()
			l:= -1
			if(len(rf.log)>0){
				l= rf.log[len(rf.log)-1].Term
			}
			sendVoteArgs := &RequestVoteArgs{Term:rf.currentTerm,
											 CandidateId:rf.me,
											 LastLogIndex: len(rf.log),
											 LastLogTerm: l,
										}
			sendVoteReply := &RequestVoteReply{}
			rf.mux.Unlock()
			go rf.sendVR(i, sendVoteArgs, sendVoteReply, votes)
		}
	}
	t := rf.currentTerm
	go rf.analyzeVotes(votes, t)
	return	
}

func (rf *Raft) resetTimeout(){
	// should this be for select?
	//fmt.Printf("resetTimeout is called %d %d\n", rf.me, rf.currentState )
	rand.Seed(int64(rf.me))
	l := rand.Intn(150) + 150
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
					rf.currentState = 2
					rf.votedFor = rf.me
					go rf.newElection()
				} else if (rf.currentState == 2){
					//fmt.Printf("Already a candidate and starts election %d\n", rf.me)
					go rf.newElection()
				}
				rf.mux.Unlock()
		}
  		
	}
	return
}


func (rf *Raft) applyEntries() {
	fmt.Printf("apply entries called on id %d commit index is %d lastApplied is %d\n", rf.me, rf.commitIndex, rf.lastApplied)
	//rf.mux.Lock()
	//defer rf.mux.Unlock()
	m := make(chan ApplyMsg, rf.commitIndex - rf.lastApplied)
	applyMsgChan := rf.applyCh
	go func() {
		for applyMsg := range m {
			applyMsgChan <- applyMsg
		}
		return
	}()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		command := rf.log[rf.lastApplied-1].Command
		i := rf.lastApplied
		//fmt.Printf("rf %v send committedIndex %v, command %v\n", rf.me, i, command)
		//fmt.Printf("rf %v lastApplied is %v\n", rf.me, rf.lastApplied)
		m <- ApplyMsg{
			Index: i,
			Command: command,

		}
		//rf.lastApplied += 1
	}
	return
}

func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	//fmt.Printf("Make called on id %d\n", me);
	rf := &Raft{}
	rf.mux.Lock()
	fmt.Printf("took lock in make id is %d\n", me)
	rf.peers = peers
	rf.me = me
	//fmt.Printf("Make called %d\n", rf.me);
	//what is supposed to happen in make? 
	//lock here??

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.currentState = 1 //does raft start off as a follower
	rf.applyCh = applyCh
	n := make([]int, len(rf.peers))
	m := make([]int, len(rf.peers))
	rf.nextIndex = n
	rf.matchIndex = m 
	//what is supposed to happen in make? 
	go rf.resetTimeout()
	
	fmt.Printf("released lock in make id is %d\n", me)
	rf.mux.Unlock()
	// Your initialization code here (2A, 2B)

	return rf
}
