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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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
type logEntry struct {
	Command int
	Term    int
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	log           []logEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	heardBeat     bool
	isleader      int
	heartbeatchan chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.isleader == 3)
	// Your code here (2A).
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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

//
// example RequestVote RPC handler.
//
type appendEntryArg struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	Entries      []logEntry
	LeaderCommit int
}

type appendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *appendEntryArg, reply *appendEntryReply) {

	// Your code here (2A, 2B).
	rf.heardBeat = true
	rf.votedFor = -1
	// fmt.Printf("raft%d 接收心跳从raft%d，其心跳状态是%v\n", rf.me, args.LeaderId, rf.heardBeat)
	if rf.me != args.LeaderId {
		rf.isleader = 1
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	if args.Term < rf.currentTerm || args.PreLogIndex >= len(rf.log) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.log[args.PreLogIndex].Term != args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	newentryIndex := 0
	logIndex := args.PreLogIndex + 1
	for ; newentryIndex < len(args.Entries); newentryIndex++ {
		if logIndex >= len(rf.log) {
			break
		}
		if (rf.log[logIndex].Term) != args.Entries[newentryIndex].Term {
			break
		}
		logIndex++
	}
	if logIndex <= args.PreLogIndex+len(args.Entries) {
		rf.log = rf.log[0:logIndex]
		newentryIndex = logIndex - args.PreLogIndex - 1
		for ; newentryIndex < len(args.Entries); newentryIndex++ {
			rf.log = append(rf.log, args.Entries[newentryIndex])
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if args.LastLogIndex >= len(rf.log)-1 {
				fmt.Printf("origin:raft%d agreed by %d\n", args.CandidateId, rf.me)
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				return
			} else {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				return
			}
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

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

	return index, term, isLeader
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
func (rf *Raft) peroidRequestVote() {
	var timeout int
	for true {
		timeout = int((0.7 + rand.Float32()*0.3) * 300)
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		if (!rf.heardBeat) && rf.isleader == 1 {
			fmt.Printf("raft%d start election,其心跳状态是%v\n", rf.me, rf.heardBeat)
			go rf.Vote()
			rf.isleader = 2
		}
		rf.heardBeat = false
	}
}

func (rf *Raft) Vote() {
	hasvote := 0
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	if len(rf.log) == 0 {
		args.LastLogIndex = -1
		args.LastLogTerm = -1
	} else {
		args.LastLogIndex = rf.log[len(rf.log)-1].Command
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	for i := 0; i < len(rf.peers); i++ {
		replys := RequestVoteReply{}
		rf.peers[i].Call("Raft.RequestVote", &args, &replys)
		if replys.Term > args.Term {
			rf.currentTerm = replys.Term
			rf.isleader = 1
			rf.heardBeat = false
			return
		}
		if replys.VoteGranted {
			fmt.Printf("raft%d agreed by %d\n", rf.me, i)
			hasvote += 1

		}
		if 2*hasvote >= len(rf.peers) {
			fmt.Printf("raft%d完成选举\n", rf.me)
			rf.isleader = 3
			rf.currentTerm += 1
			go rf.leaderheartbeat()
			break
		}
	}
}

func (rf *Raft) leaderheartbeat() {
	for true {
		args := appendEntryArg{}
		args.Entries = make([]logEntry, 0)
		args.LeaderCommit = rf.commitIndex
		args.LeaderId = rf.me
		args.PreLogIndex = rf.lastApplied
		args.Term = rf.currentTerm
		for i := 0; i < len(rf.peers); i++ {
			replys := appendEntryReply{}
			rf.peers[i].Call("Raft.AppendEntry", &args, &replys)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Printf("create a new raft%d,peer是有%d个\n", me, len(peers))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.isleader = 1
	rf.votedFor = -1
	rf.heardBeat = true
	rf.heartbeatchan = make(chan struct{})

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.peroidRequestVote()
	return rf
}
