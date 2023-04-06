package mr

import (
	"6.5840/lablog"
	"encoding/gob"
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type timeSec int64
type Coordinator struct {

	//for task management
	taskChannel    chan *Task //tasks to be assigned in this stage
	tasks          map[int]*Task
	taskSituations map[int]*taskSituation // situation of all tasks in the current stage
	stage          int
	stageLock      sync.RWMutex

	//for worker management
	lockMaxWorkerInd sync.Mutex
	maxWorkerInd     int

	wg sync.WaitGroup //to count the tasks left for the current stage

	lockDone sync.Mutex
	done     bool
}

type taskSituation struct {
	situation int
	asignTime timeSec
	worker    int
	mu        sync.Mutex
}

func (c *Coordinator) addMaxWorkerInd() int {
	c.lockMaxWorkerInd.Lock()
	defer c.lockMaxWorkerInd.Unlock()
	c.maxWorkerInd++
	return c.maxWorkerInd
}

func (c *Coordinator) getMaxWorkerInd() int {
	c.lockMaxWorkerInd.Lock()
	defer c.lockMaxWorkerInd.Unlock()
	return c.maxWorkerInd
}

//functions for the coordinator to use, but not expose to RPC

func checkTimeOut(c *Coordinator) {
	for {
		//lock to not allow stage change
		c.stageLock.RLock()
		for taskid, situation := range c.taskSituations {
			situation.mu.Lock()
			if situation.situation == 1 {
				now := timeSec(time.Now().Unix())
				if now-situation.asignTime > 10 {
					situation.situation = 0
					c.taskChannel <- c.tasks[taskid]
					lablog.Debug(lablog.Drop, "time out for task v%-v%, worker v%", c.stage, taskid, situation.worker)
				}
			}
			situation.mu.Unlock()
		}
		c.stageLock.RUnlock()
		time.Sleep(3 * time.Second)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterWorker(args *int, workerID *int) error {
	c.lockDone.Lock()
	if c.done {
		*workerID = -1
		return nil
	}
	c.lockDone.Unlock()

	*workerID = c.addMaxWorkerInd()
	return nil
}

func (c *Coordinator) AsignTask(args *int, task *Task) error {
	//check if the whole application is already done
	c.lockDone.Lock()
	if c.done {
		*task = Task{Stage: -1}
		c.lockDone.Unlock()
		return nil
	}
	c.lockDone.Unlock()

	*task = c.asignTask(*args)
	defer lablog.Debug(lablog.Leader, "assign task %v-%v to worker %v", task.Stage, task.TaskID, *args)
	//must be done
	if task.Stage == -1 {
		lablog.Debug(lablog.Leader, "all finished,quit worker %v", *args)
	}
	return nil
}

//asign a task and register in the coordinator
func (c *Coordinator) asignTask(id int) Task {
	//if t can get one, then it means the current stage isn't finished so we can do
	//task asignment without worrying about the c.taskSituations or the c.tasks being rebuilt
	var t *Task = nil
	for t = range c.taskChannel {
		c.taskSituations[t.TaskID].mu.Lock()
		if c.taskSituations[t.TaskID].situation == 0 {
			break
		}
		c.taskSituations[t.TaskID].mu.Unlock()
	}
	if t == nil {
		return Task{Stage: -1}
	}
	c.taskSituations[t.TaskID].situation = 1
	c.taskSituations[t.TaskID].worker = id
	c.taskSituations[t.TaskID].asignTime = timeSec(time.Now().Unix())
	c.taskSituations[t.TaskID].mu.Unlock()
	return *t
}

func (c *Coordinator) FinishTask(args *FinishReport, nextTask *Task) error {
	c.lockDone.Lock()
	if c.done {
		*nextTask = Task{Stage: -1}
		c.lockDone.Unlock()
		lablog.Debug(lablog.Leader, "all finished,quit worker %v", args.WorkerID)
		return nil
	}
	c.lockDone.Unlock()

	//validation the finished task is the registered task and then register finished
	c.stageLock.RLock()
	if c.stage == args.Stage {
		situation := c.taskSituations[args.TaskID]
		situation.mu.Lock()
		if situation.worker == args.WorkerID && situation.situation == 1 {
			if args.Succeed {
				situation.situation = 2
				c.wg.Done()
			} else {
				situation.situation = 0
				c.taskChannel <- c.tasks[args.TaskID]
			}
		}
		situation.mu.Unlock()
	}
	c.stageLock.RUnlock()
	//asign next task
	*nextTask = c.asignTask(args.WorkerID)
	lablog.Debug(lablog.Leader, "assign task %v-%v to worker %v", (*nextTask).Stage, (*nextTask).TaskID, (*args).WorkerID)
	//must be done
	if nextTask.Stage == -1 {
		lablog.Debug(lablog.Leader, "all finished,quit worker %v", *args)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	gob.Register(WcReduceContent{})
	gob.Register(WcMapContent{})
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

}

func (c *Coordinator) stageManager(nReduce int, initialTaskNum int) {
	for _, task := range c.tasks {
		c.taskChannel <- task
	}

	c.wg.Wait()
	lablog.Debug(lablog.Leader, "stage 0 finished")

	c.stageLock.Lock()
	c.stage = 1
	//clear the taskSituation map and the tasks
	c.taskSituations = make(map[int]*taskSituation)
	c.tasks = make(map[int]*Task)

	//change to the new tasks
	for i := 0; i < nReduce; i++ {
		wcReduceTask := Task{
			Stage: 1, TaskID: i, Content: WcReduceContent{MapNum: initialTaskNum}}
		c.taskSituations[i] = &taskSituation{
			situation: 0,
			asignTime: 0,
			worker:    -1,
			mu:        sync.Mutex{},
		}
		c.tasks[i] = &wcReduceTask
		c.wg.Add(1)
	}
	c.stageLock.Unlock()
	lablog.Debug(lablog.Leader, "stage changed successfully")

	for _, task := range c.tasks {
		c.taskChannel <- task
	}

	//wait for the next stage to finish
	c.wg.Wait()
	close(c.taskChannel)

	//all the tasks are done
	c.lockDone.Lock()
	c.done = true
	c.lockDone.Unlock()
	lablog.Debug(lablog.Leader, "the job is finished")
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lockDone.Lock()
	ret := c.done
	c.lockDone.Unlock()
	if ret {
		files, err := filepath.Glob("wc-*")
		if err != nil {
			panic(err)
		}
		for _, f := range files {
			if err := os.Remove(f); err != nil {
				panic(err)
			}
		}
		//time for notifying workers to stop
		time.Sleep(time.Second)
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	//initialize the coordinator
	c := Coordinator{
		taskChannel:      make(chan *Task),
		tasks:            make(map[int]*Task),
		taskSituations:   make(map[int]*taskSituation),
		stage:            0,
		stageLock:        sync.RWMutex{},
		lockMaxWorkerInd: sync.Mutex{},
		maxWorkerInd:     -1,
		wg:               sync.WaitGroup{},
		lockDone:         sync.Mutex{},
		done:             false,
	}

	//initialize the tasks and stage
	for i, file := range files {
		wcMapTask := Task{
			Stage: 0, TaskID: i, Content: WcMapContent{Filename: file, NReduce: nReduce}}
		c.taskSituations[i] = &taskSituation{
			situation: 0,
			asignTime: 0,
			worker:    -1,
			mu:        sync.Mutex{},
		}
		c.tasks[i] = &wcMapTask
		c.wg.Add(1)
	}

	go c.stageManager(nReduce, len(files))
	c.server()
	lablog.Debug(lablog.Leader, "coordinator initialized done")

	//check the heartbeat table every three seconds
	go checkTimeOut(&c)

	return &c
}
