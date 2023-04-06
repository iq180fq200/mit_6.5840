package mr

import (
	"6.5840/lablog"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//register
	workerID := CallRegisterWorker()
	lablog.Debug(lablog.Client, "W%d Register succeed", int(workerID))

	//ask for the first task
	task := CallAsignTask(workerID)
	//lablog.Debug(lablog.Client, "W%d Get a task", int(workerID))
	lablog.Debug(lablog.Info, "W%d get task, stage %d, task id %d", int(workerID), int(task.Stage), int(task.TaskID))

	//ask for the job and report result
	for {
		//judge the task type and finish the task
		switch task.Stage {
		case 0: //map
			filename := task.Content.(WcMapContent).Filename
			lablog.Debug(lablog.Info, "W%d map task begin, file name %v", int(workerID), filename)
			nReduce := task.Content.(WcMapContent).NReduce
			file, err := os.Open(filename)
			if err != nil {
				//log.Fatalf("cannot open %v", filename)
				lablog.Debug(lablog.Error, "W%d cannot open %v", int(workerID), filename)
				os.Exit(1)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				//log.Fatalf("cannot read %v", filename)
				lablog.Debug(lablog.Error, "W%d cannot read %v", int(workerID), filename)
				os.Exit(1)
			}
			file.Close()
			kva := mapf(filename, string(content))
			//create nReduce temp files
			var encList []*json.Encoder
			var tmpFiles []*os.File
			for i := 0; i < nReduce; i++ {
				//create and open the mediate file
				tmpFile, err := ioutil.TempFile("", "wc-"+strconv.Itoa(int(task.TaskID))+"-"+strconv.Itoa(i))
				if err != nil {
					lablog.Debug(lablog.Error, "W%d cannot create temporary file", int(workerID))
					os.Exit(1)
				}
				enc := json.NewEncoder(tmpFile)
				encList = append(encList, enc)
				tmpFiles = append(tmpFiles, tmpFile)
			}
			//write data
			for _, kv := range kva {
				i := ihash(kv.Key) % nReduce
				enc := encList[i]
				err := enc.Encode(&kv)
				if err != nil {
					lablog.Debug(lablog.Error, "W%d cannot write to temp file", int(workerID))
					os.Exit(1)
				}
			}
			//rename the files and then close them
			for i := 0; i < nReduce; i++ {
				//tmpFiles[i].Close()
				mediate_file_name := "wc-" + strconv.Itoa(int(task.TaskID)) + "-" + strconv.Itoa(i)
				err := os.Rename(tmpFiles[i].Name(), mediate_file_name)
				if err != nil {
					//log.Fatalf("cannot rename to %v", mediate_file_name)
					lablog.Debug(lablog.Error, "W%d cannot rename to %v", int(workerID), mediate_file_name)
					os.Exit(1)
				}
				tmpFiles[i].Close()
			}

		case 1: //reduce
			var filenames []string
			for i := 0; i < task.Content.(WcReduceContent).MapNum; i++ {
				filenames = append(filenames, "wc-"+strconv.Itoa(i)+"-"+strconv.Itoa(int(task.TaskID)))
			}
			var kva []KeyValue
			for _, filename := range filenames {
				file, err := os.Open(filename)
				defer file.Close()
				if err != nil {
					lablog.Debug(lablog.Error, "W%d cannot open %v", int(workerID), filename)
					os.Exit(1)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(int(task.TaskID))
			ofile, _ := ioutil.TempFile("", oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()
		}
		//tell him I am finished and get the new task
		lablog.Debug(lablog.Client, "W%v succeed task %v-%v", workerID, task.Stage, task.TaskID)
		task = CallFinishTask(workerID, task.TaskID, task.Stage, true)
		lablog.Debug(lablog.Info, "W%d get task, stage %d, task id %d", workerID, task.Stage, task.TaskID)
	}
}

func CallRegisterWorker() int {
	// declare an argument structure.
	var reply int
	anything := 1

	ok := call("Coordinator.RegisterWorker", &anything, &reply)
	if ok {
		if reply == -1 {
			lablog.Debug(lablog.Client, "job is done! exit worker without registering!")
			os.Exit(0)
		}
		return reply
	} else {
		lablog.Debug(lablog.Error, "call register worker failed")
		os.Exit(1)
	}
	return reply
}

func CallAsignTask(id int) *Task {
	// declare an argument structure.
	var reply Task
	ok := call("Coordinator.AsignTask", &id, &reply)
	if reply.Stage == -1 {
		lablog.Debug(lablog.Client, "job is done! exit worker %v\n", id)
		os.Exit(0)
	}
	if ok {
		return &reply
	} else {
		lablog.Debug(lablog.Error, "W%v call asign task failed", id)
		os.Exit(1)
	}
	return &reply
}

func CallFinishTask(id int, id1 int, stage int, succeed bool) *Task {
	report := FinishReport{
		WorkerID: id,
		TaskID:   id1,
		Stage:    stage,
		Succeed:  succeed,
	}
	var nextTask Task
	// declare an argument structure.
	ok := call("Coordinator.FinishTask", &report, &nextTask)
	if nextTask.Stage == -1 {
		lablog.Debug(lablog.Client, "job is done! exit worker %v\n", id)
		os.Exit(0)
	}
	if ok {
		return &nextTask
	} else {
		lablog.Debug(lablog.Error, "W%v call finish task failed", id)
		os.Exit(1)
	}
	return &nextTask
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	gob.Register(WcReduceContent{})
	gob.Register(WcMapContent{})
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
