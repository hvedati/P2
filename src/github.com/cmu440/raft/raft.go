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
    }

    var t int
    if (len(rf.log)>0){
        t= rf.log[len(rf.log)- 1].Term 
    }
   // i := len(rf.log) -1
    check := false
    if (args.LastLogTerm > t){
        check = true
    }
    if (args.LastLogTerm == t && args.LastLogIndex >= (len(rf.log)-1)){
        check = true
    }
    if (len(rf.log) <= 0){
        check = true
    }
    if (rf.votedFor == -1){
        if (check){
            rf.currentState = 1
            reply.VoteGranted = true
            rf.votedFor = args.CandidateId
        }
    }else if (rf.votedFor  == args.CandidateId){
        if (check){
            rf.currentState = 1
            reply.VoteGranted = true
            rf.votedFor = args.CandidateId
        }
    }    
    return
}


func (rf *Raft) updateLog(args *AppendEntriesArgs){
    for i, e := range args.Entries{
        if((args.PrevLogIndex + i) >0){
            if(len(rf.log) > args.PrevLogIndex + i){
                if(rf.log[args.PrevLogIndex + i].Term != e.Term){
                    rf.log = rf.log[:args.PrevLogIndex + i]
                }
            }
        }
        rf.log = append(rf.log, e)
    }

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
            //fmt.Printf("follower reseting the timeout\n")
            rf.resetTimeout()

        }
        if (rf.currentState == 2){
            //fmt.Printf("candidate reseting the timeout\n")
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

    rf.updateLog(args)
    //fmt.Printf("the leader commit is %d and the commit index is %d\n ", args.LeaderCommit, rf.commitIndex)
    if(args.LeaderCommit > rf.commitIndex){
       if (len(args.Entries) >0 && args.LeaderCommit > args.PrevLogIndex + len(args.Entries)){
            rf.commitIndex = args.PrevLogIndex + len(args.Entries)
        }else{
            rf.commitIndex = args.LeaderCommit
        }
    }
    //fmt.Printf("sent to apply ch %d\n", rf.me)
    //fmt.Printf("the log for id %d is %v\n", rf.me, rf.log)
    rf.sendCommands()
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
   // fmt.Printf("Start is called on id number %d\n", rf.me)
    rf.mux.Lock()
   
    defer rf.mux.Unlock()
    index := -1
    term := rf.currentTerm
    isLeader := true
    if (rf.currentState != 0){
        isLeader = false
    }
    if (isLeader){
        //fmt.Printf("START is called on id number %d command is%v\n", rf.me, command)
        newEntry := logEntry{Command: command, 
                             Term: rf.currentTerm,}
        rf.log = append(rf.log, newEntry)
        index = len(rf.log)
        //fmt.Printf("rf %v receive command %v, index is %v log is %v\n", rf.me, command, index, rf.log)
        rf.sendAE()
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
        //rf.mux.Unlock()
        result := rf.sendAppendEntries(i, a, r)
        //fmt.Printf("heart beat sent to %d from %d and result was %t\n",i, rf.me, result )
        if(result){
            //rf.mux.Lock()
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
        }else{
            rf.mux.Unlock()
        }
        time.Sleep(time.Millisecond * 100)
}
}

func (rf *Raft) majorityReceived(majority chan bool, cIndex int){

}
func (rf *Raft) actuallySend(c int, i int){
    for{
        rf.mux.Lock()
        if(rf.currentTerm != c || rf.currentState != 0){
                    rf.mux.Unlock()
                    return
        }

        pLIndex := rf.nextIndex[i] -1
        var pLTerm int
        var e []logEntry
        if (pLIndex < len(rf.log)){
            pLTerm = rf.log[pLIndex].Term
            e = rf.log[rf.nextIndex[i] - 1:]
            a := &AppendEntriesArgs{Term:rf.currentTerm,
                                            LeaderId:rf.me,
                                            PrevLogIndex: pLIndex,
                                            PrevLogTerm:pLTerm,
                                            Entries: e,
                                            LeaderCommit:rf.commitIndex,
                                            }
            r := &AppendEntriesReply{}
            result := rf.sendAppendEntries(i, a, r)
            needToRetry := false
            if(result){
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
                if (r.Success== true){
                    //fmt.Printf("result is a SUCCESS for id %d length of entries is %d entries are %v\n", i, len(a.Entries), a.Entries)
                    rf.nextIndex[i] = rf.nextIndex[i] + len(a.Entries)
                    rf.matchIndex[i] = rf.nextIndex[i] - 1
                    c := rf.commitIndex
                    l := len(rf.log) 
                    for i := c+1; i <= l; i++{
                        count := 1
                        for p := range rf.peers {
                            if (p != rf.me  && rf.matchIndex[p] >= i){
                                count +=1
                            }
                        }
                        if count > len(rf.peers)/2 {
                            rf.commitIndex = i
                            rf.sendCommands()
                        }
                    }
                }else{
                    rf.nextIndex[i] = rf.nextIndex[i] - 1
                    needToRetry = true
                }
                
            }else{
               
            }
            if (needToRetry == false){
               rf.mux.Unlock()
               return
            }else{
                rf.mux.Unlock()
            }
        }else{
          rf.mux.Unlock()
        }

        
    }

}


func (rf *Raft) sendAE(){
    //fmt.Printf("send AE called on server %d\n", rf.me)
    c := rf.currentTerm
    for i := range(rf.peers){
        if (i != rf.me){
            go rf.actuallySend(c, i)
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
        if ((count + 1) > totalPeers/2){
            //fmt.Printf("Became a leader %d\n", rf.me)
            rf.currentState = 0
            l := len(rf.log)
            for i:= range rf.peers {
                if (i != rf.me){
                    rf.nextIndex[i] = l + 1
                }
            }
            for i:= range rf.peers {
                if (i != rf.me){
                     go rf.sendHeartBeats(rf.currentTerm,i)
                }
            }
            //go rf.sendHeartBeats(c,i)
            
            rf.mux.Unlock()
            return           
        }
        rf.mux.Unlock()        
    }
    
}


func (rf *Raft) newElection(){
    //fmt.Printf("new election called %d\n", rf.me)
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
    l := rand.Intn(400) + 300
    timer := time.NewTimer(time.Duration(l) * time.Millisecond)
    //rf.mux.Lock()
    t := rf.currentTerm
    go rf.runTimer(t, timer)
}

func (rf *Raft) sendToApply(p chan ApplyMsg){
    for applyMsg := range p {
        //tfmt.Printf("sent tot apply channel %d message is %v\n", rf.me, applyMsg)
        rf.applyCh<- applyMsg
    }
    return

}
func (rf *Raft) sendCommands() {
    //fmt.Printf("send entries called on %d commit index is %d last applied is %d\n", rf.me, rf.commitIndex, rf.lastApplied)
    passThrough := make(chan ApplyMsg)
    go rf.sendToApply(passThrough)

    for i := rf.lastApplied; i < rf.commitIndex + 1; i++ {
        if (i < len(rf.log)){
            command := rf.log[i].Command
            passThrough <- ApplyMsg{
                Index: i+1,
                Command: command,

            }
            rf.lastApplied = i
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
