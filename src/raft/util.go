package raft

import "log"

//HeartBeats
const HeartBeatsMill =100
//Apply Loop Check Time
const ApplyLoopCheckMill=10

// Debugging
const Enable =true
const Disable =false

const Debug = Disable
const Debug2 = Disable
const ApplyLogOutput = Enable

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug2 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf3(format string, a ...interface{}) (n int, err error) {
	if ApplyLogOutput {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft)Me() int {
	return rf.me
}

func (rf *Raft)Role() string {
	
	switch rf.role{
	case int(Follower):
		return "Follower"
	case int(Candidate):
		return "Candidate"
	case int(Leader):
		return "Leader"
	case int(PreCandidate):
		return "PreCandidate"
	}

	return "Err"
}