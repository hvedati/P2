
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
//import "fmt"
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
type logEntry struct{
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
    //fmt.Printf("took lock in get state id is %d\n", rf.me)
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
    //fmt.Printf("released lock in get state id is %d\n", rf.me)
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
    rf.mux.Lock()
	defer rf.mux.Unlock()
    reply.Term = rf.currentTerm
	if (args.Term < rf.currentTerm){
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	} else if (rf.currentTerm < args.Term){
		reply.Term = rf.currentTerm
		rf.currentState = 1
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.VoteGranted = true		
		rf.resetTimeout()
	}else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	if (rf.votedFor == -1){
		l := len(rf.log)
        //s := l <= 0 || (rf.log[l-1].Term <= args.LastLogTerm && (rf.log[l-1].Term  != args.LastLogTerm  || l <= args.LastLogIndex))
		//s:= l > 0 && (rf.log[l-1].Term > args.LastLogTerm || (rf.log[l-1].Term  == args.LastLogTerm && l > args.LastLogIndex))
		//need to check if its bigger than 0 
		if(l <= 0){		
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
		}else if(rf.log[l-1].Term <= args.LastLogTerm && (rf.log[l-1].Term  != args.LastLogTerm  || l <= args.LastLogIndex)){
            reply.VoteGranted = true
            rf.votedFor = args.CandidateId
            reply.Term = rf.currentTerm
        }
	}else if (rf.votedFor  == args.CandidateId){
		l := len(rf.log)
		if(l <= 0){      
                reply.VoteGranted = true
                rf.votedFor = args.CandidateId
                reply.Term = rf.currentTerm
        }else if(rf.log[l-1].Term <= args.LastLogTerm && (rf.log[l-1].Term  != args.LastLogTerm  || l <= args.LastLogIndex)){
            reply.VoteGranted = true
            rf.votedFor = args.CandidateId
            reply.Term = rf.currentTerm
        }
	}
	
	if(reply.VoteGranted == true && rf.currentState == 1){
		rf.resetTimeout()
	}
	return
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mux.Lock()
	defer rf.mux.Unlock()
	//reply.Success = true
	if(args.Term < rf.currentTerm){
		reply.Term = rf.currentTerm
		reply.Success = false
		
	}else if(args.Term == rf.currentTerm){
		if (rf.currentState == 1){
			reply.Term = rf.currentTerm
			reply.Success = true
			//fmt.Printf("follower reseting the timeout")
			rf.resetTimeout()

		}
		if (rf.currentState == 2){
			//fmt.Printf("candidate reseting the timeout")
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.currentState = 1
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.resetTimeout()
		}else{
			reply.Term = rf.currentTerm
			reply.Success = true
		}
	} else{
		reply.Term = rf.currentTerm
		rf.currentState = 1
		rf.currentTerm = args.Term
		rf.votedFor = -1	
		reply.Success = true
		rf.resetTimeout()	
	}

	//
	check := args.PrevLogIndex<= 0 || len(rf.log)  < args.PrevLogIndex
	//fmt.Printf("The id is %d, the prev log index is %d, the len of the log is %d\n",rf.me, args.PrevLogIndex, len(rf.log))
	if (args.PrevLogIndex > 0){
        if(check || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm){
		reply.Success = false
    }
	}
	if(reply.Success == false){
		return
	}

	for i, e := range args.Entries{
		if((args.PrevLogIndex + 1 + i) >0){
            if(len(rf.log) >= args.PrevLogIndex + 1 + i){
    			if(rf.log[args.PrevLogIndex + i].Term != e.Term){
    				rf.log = rf.log[:args.PrevLogIndex + i]
    			}
            }
		}
		rf.log = append(rf.log, e)
	}

	if(args.LeaderCommit > rf.commitIndex){
		//check if lene args.entries is greater than 0
		if (len(args.Entries) >0 && args.LeaderCommit > args.PrevLogIndex + len(args.Entries)){
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}else{
			rf.commitIndex = args.LeaderCommit
		}
	}
	rf.sendEntries()
	return
	// deal with heartbeat here? */
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
	defer rf.mux.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := true
	if (rf.currentState != 0){
		isLeader = false
	}
	if (isLeader){
		newEntry := logEntry{Command: command, 
							 Term: rf.currentTerm,}
		rf.log = append(rf.log, newEntry)
		index = len(rf.log)
		//fmt.Printf("rf %v receive command %v, index is %v ", rf.me, command, index)
	}
	return index, term, isLeader
}


func (rf *Raft) Kill() {
	// Your code here, if desired
}

func (rf *Raft) sendHeartBeats(c int, i int){
    for{
        rf.mux.Lock()
        if(rf.currentTerm != c || rf.currentState != 0){
                    rf.mux.Unlock()
                    return
        }

        pLIndex := rf.nextIndex[i] -1
        pLTerm := -1
        var e []logEntry

        if(pLIndex >0 && len(rf.log)>= pLIndex){
            pLTerm = rf.log[pLIndex-1].Term
        }
        a := &AppendEntriesArgs{Term:rf.currentTerm,
                                        LeaderId:rf.me,
                                        PrevLogIndex: pLIndex,
                                        PrevLogTerm:pLTerm,
                                        Entries: e,
                                        LeaderCommit:rf.commitIndex,
                                        }
        r := &AppendEntriesReply{}
        rf.mux.Unlock()
        result := rf.sendAppendEntries(i, a, r)
        
        if(result){
            rf.mux.Lock()
            if (r.Term > rf.currentTerm){
                rf.currentState = 1
                rf.currentTerm = r.Term
                rf.votedFor = -1
                rf.resetTimeout()
            }
            if(rf.currentState != 0){
                rf.mux.Unlock()
                return
            }
            rf.mux.Unlock()
        }
        time.Sleep(time.Millisecond * 100)
}
}
func (rf *Raft) actuallySend(c int, i int){
	for{
		rf.mux.Lock()
		if(rf.currentTerm != c || rf.currentState != 0){
					rf.mux.Unlock()
					return
		}

		pLIndex := rf.nextIndex[i] -1
		pLTerm := -1
		var e []logEntry

		if(pLIndex >0 && len(rf.log)>= pLIndex){
			pLTerm = rf.log[pLIndex-1].Term
			e = rf.log[rf.nextIndex[i] - 1:]
		}else{
			e = rf.log
		}
		a := &AppendEntriesArgs{Term:rf.currentTerm,
										LeaderId:rf.me,
										PrevLogIndex: pLIndex,
										PrevLogTerm:pLTerm,
										Entries: e,
										LeaderCommit:rf.commitIndex,
										}
		r := &AppendEntriesReply{}
		rf.mux.Unlock()
		result := rf.sendAppendEntries(i, a, r)
		//s := true
		if(result){
			rf.mux.Lock()
			if (r.Term > rf.currentTerm){
				rf.currentState = 1
				rf.currentTerm = r.Term
				rf.votedFor = -1
				rf.resetTimeout()
			}
			if(rf.currentState != 0){
				rf.mux.Unlock()
				return
			}
			if (r.Success){
				rf.nextIndex[i] = rf.nextIndex[i] + len(a.Entries)
				rf.matchIndex[i] = rf.nextIndex[i] - 1
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
				}
				rf.sendEntries()
			}else{
				if rf.nextIndex[i] > 1 {
                    rf.nextIndex[i] = rf.nextIndex[i] - 1
                }
                //s = false
			}
			rf.mux.Unlock()
		}

		//if (s){
          ///  time.Sleep(time.Millisecond * 50)
		//}
	}

}


func (rf *Raft) sendAE(){
	//fmt.Printf("send AE called")
	c := rf.currentTerm
	for i,_ := range(rf.peers){
		if (i != rf.me){
			go rf.actuallySend(c, i)
            go rf.sendHeartBeats(c,i)
		}
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
				rf.mux.Lock()
				rf.currentTerm = r.Term
				rf.currentState = 1
				rf.votedFor = -1	
				rf.resetTimeout()
						
			}
            rf.mux.Unlock()
			c <- false
		}
	
	}
    return
}


func (rf *Raft) analyzeVotes(c chan bool, t int){
	totalPeers := len(rf.peers)
	count := 0
	for elem := range(c){
		rf.mux.Lock()
		
		// check if the term is the same by locking?
		if(rf.currentTerm != t){
			rf.mux.Unlock()
			return
		}
		if (elem == true){
			//fmt.Printf("inside the elem is true case for the id %d\n", rf.me)
			count += 1 
		}
		//if (rf.currentState != 2){
		//	rf.mux.Unlock()
		//	return
		//}
		if ((count + 1) > totalPeers/2){
			//fmt.Printf("Became a leader %d\n", rf.me)
			rf.currentState = 0
			l := len(rf.log)
			for i:= range rf.peers {
				if (i != rf.me){
					rf.nextIndex[i] = l + 1
				}
			}
			//fmt.Printf("leader %v log is %+v, nextIndex is %+v\n", rf.me, rf.log, rf.nextIndex)
			//rf.votedFor = rf.me
			rf.sendAE()
			
			rf.mux.Unlock()
			return 
			//start go routine that sends heatbeats //only do this if it is still in a valid state
			// need to end this go routine at some point			
		}
		rf.mux.Unlock()		
	}
	
}


func (rf *Raft) newElection(){
	rf.currentTerm = rf.currentTerm +1
	numPeers := len(rf.peers)
	votes := make(chan bool, numPeers) // buffered channel? 
	c := rf.currentTerm
	for i,_ := range(rf.peers){
		if (i != rf.me){
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
			go rf.sendVR(i, sendVoteArgs, sendVoteReply, votes)
		}
	}
	//t := rf.currentTerm
	go rf.analyzeVotes(votes, c)
	return	
}

func (rf* Raft) runTimer(t int, timer *time.Timer){
    select{
        case <-timer.C:
            rf.mux.Lock()
            defer rf.mux.Unlock()

            if rf.currentTerm != t {
                return
            }
            if (rf.currentState == 1){
                rf.currentState = 2
                rf.votedFor = rf.me
                rf.newElection()
            } else if (rf.currentState == 2){
                rf.newElection()
            }
    }

}

func (rf *Raft) resetTimeout(){
	rand.Seed(int64(rf.me))
	l := rand.Intn(300) + 300
	timer := time.NewTimer(time.Duration(l) * time.Millisecond)
	//rf.mux.Lock()
	t := rf.currentTerm
    go rf.runTimer(t, timer)
}

func (rf *Raft) sendToApply(m chan ApplyMsg){
    for applyMsg := range m {
                rf.applyCh<- applyMsg
    }

}
func (rf *Raft) sendEntries() {
	m := make(chan ApplyMsg, rf.commitIndex - rf.lastApplied)
    go rf.sendToApply(m)

    for i := rf.lastApplied; i < rf.commitIndex; i++ {
        command := rf.log[i].Command
        m <- ApplyMsg{
            Index: i+1,
            Command: command,

        }
    }
	return
}

func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	//fmt.Printf("Make called on id %d\n", me);
	rf := &Raft{}
	rf.mux.Lock()
	defer rf.mux.Unlock()
	rf.peers = peers
	rf.me = me
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.currentState = 1 //does raft start off as a follower
	rf.applyCh = applyCh
	n := make([]int, len(rf.peers))
	m := make([]int, len(rf.peers))
	rf.nextIndex = n
	rf.matchIndex = m 
	//what is supposed to happen in make? 
	rf.resetTimeout()
	
	//rf.mux.Unlock()
	// Your initialization code here (2A, 2B)

	return rf
}