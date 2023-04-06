package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type Task struct {
	TaskID  int
	Stage   int
	Content interface{}
}
type WcMapContent struct {
	Filename string
	NReduce  int
}
type WcReduceContent struct {
	MapNum int
}

type FinishReport struct {
	WorkerID int
	TaskID   int
	Stage    int
	Succeed  bool
}

type AllFinished bool
type Status bool

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
