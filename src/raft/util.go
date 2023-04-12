package raft

//
//import "log"
//
//// Debugging
//const Debug = false
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug {
//		log.Printf(format, a...)
//	}
//	return
//}
import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
// ref: https://blog.josejg.com/debugging-pretty/
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type LogTopic string

const (
	dClient   LogTopic = "CLNT"
	dCommit   LogTopic = "CMIT"
	dConfig   LogTopic = "CONF"
	dCtrler   LogTopic = "SCTR"
	dDrop     LogTopic = "DROP"
	dError    LogTopic = "ERRO"
	dHeart    LogTopic = "HART"
	dInfo     LogTopic = "INFO"
	dLeader   LogTopic = "LEAD"
	dLog      LogTopic = "LOG1"
	dLog2     LogTopic = "LOG2"
	dMigrate  LogTopic = "MIGR"
	dPersist  LogTopic = "PERS"
	dSnap     LogTopic = "SNAP"
	dServer   LogTopic = "SRVR"
	dTerm     LogTopic = "TERM"
	dTest     LogTopic = "TEST"
	dTimer    LogTopic = "TIMR"
	dTrace    LogTopic = "TRCE"
	dVote     LogTopic = "VOTE"
	dWarn     LogTopic = "WARN"
	dLock     LogTopic = "LOCK"
	dInstruct LogTopic = "INST"
)

var debugStart time.Time
var debugVerbosity int
var Debug_ int

func Init() {
	//set debug to be true
	Debug_ = 1
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	// disable datetime logging
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity == 0 {
		return
	}
	if Debug_ >= 1 {
		//if topic == dLock || topic == dHeart {
		//	return
		//}
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	} else {
		if topic != dInstruct {
			return
		}
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
