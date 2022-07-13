package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskTrack struct {
	task      Task
	timestamp int64
	workerId  uint64
}

func (track *TaskTrack) expired() bool {
	return track.timestamp+10 < time.Now().Unix()
}

const (
	FILE_UNALLOCATED = 0
	FILE_OCCUPYING   = uint64(0xFFFFFFFFFFFFFFFF)
	FILE_FINISHED    = uint64(0xFFFFFFFFFFFFFFFE)
)

type Input struct {
	lock      sync.Mutex
	asso_task uint64
	value     []string
}

func (input *Input) setAssocTask(taskId uint64) {
	input.lock.Lock()
	defer input.lock.Unlock()
	input.asso_task = taskId
}

type Coordinator struct {
	nFile   int
	nReduce int

	taskIdCnt uint64

	lockTracks sync.Mutex
	taskTracks map[uint64]TaskTrack

	mapInputs []Input

	nMapResult uint64

	intermediates []Input

	nReduceResult uint64
}

func (c *Coordinator) getNextInput(inputs *[]Input) int {
	for i := range *inputs {
		input := &(*inputs)[i]
		input.lock.Lock()
		if input.asso_task == FILE_UNALLOCATED {
			input.asso_task = FILE_OCCUPYING
			input.lock.Unlock()
			return i
		}
		if input.asso_task != FILE_OCCUPYING && input.asso_task != FILE_FINISHED {
			c.lockTracks.Lock()
			task := c.taskTracks[input.asso_task]
			if task.expired() {
				task.timestamp = time.Now().Unix()
				c.lockTracks.Unlock()
				input.asso_task = FILE_OCCUPYING
				input.lock.Unlock()
				return i
			}
			c.lockTracks.Unlock()
		}
		input.lock.Unlock()
	}
	return -1
}

func (c *Coordinator) getNextMapInput() int {
	return c.getNextInput(&c.mapInputs)
}

func (c *Coordinator) getNextReduceInput() int {
	return c.getNextInput(&c.intermediates)
}

func (c *Coordinator) GenTaskId() uint64 {
	// atomically increment the taskId and return it
	return atomic.AddUint64(&c.taskIdCnt, 1)
}

func (c *Coordinator) GetParams(args *GetParamsArgs, reply *GetParamsReply) error {
	if args.Key == "nReduce" {
		reply.Value = c.nReduce
	} else if args.Key == "nFile" {
		reply.Value = c.nFile
	} else if args.Key == "nMapResult" {
		reply.Value = int(atomic.LoadUint64(&c.nMapResult))
	} else if args.Key == "nReduceResult" {
		reply.Value = int(atomic.LoadUint64(&c.nReduceResult))
	} else if args.Key == "nTask" {
		reply.Value = int(atomic.LoadUint64(&c.taskIdCnt))
	} else if args.Key == "nTaskTrack" {
		reply.Value = len(c.taskTracks)
	} else if args.Key == "nMapInput" {
		reply.Value = len(c.mapInputs)
	} else if args.Key == "nReduceInput" {
		reply.Value = len(c.intermediates)
	} else {
		reply.Value = -1
	}
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// Assign a map task to a worker
	for atomic.LoadUint64(&c.nMapResult) < uint64(c.nFile) {
		if i := c.getNextMapInput(); i != -1 {
			input := &c.mapInputs[i]
			taskId := c.GenTaskId()
			task := Task{taskId, MapTask, i, input.value}
			c.lockTracks.Lock()
			c.taskTracks[taskId] = TaskTrack{task, time.Now().Unix(), args.Whoami}
			c.lockTracks.Unlock()
			input.setAssocTask(taskId)
			reply.Task = task
			return nil
		}
		time.Sleep(time.Second)
	}

	// Assign a reduce task to a worker
	for atomic.LoadUint64(&c.nReduceResult) < uint64(c.nReduce) {
		if i := c.getNextReduceInput(); i != -1 {
			input := &c.intermediates[i]
			taskId := c.GenTaskId()
			task := Task{taskId, ReduceTask, i, input.value}
			c.lockTracks.Lock()
			c.taskTracks[taskId] = TaskTrack{task, time.Now().Unix(), args.Whoami}
			c.lockTracks.Unlock()
			input.setAssocTask(taskId)
			reply.Task = task
			return nil
		}
		time.Sleep(time.Second)
	}

	reply.Task = Task{0, RelaxTask, -1, nil}
	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	taskId := args.TaskId

	c.lockTracks.Lock()
	taskTrack, exist := c.taskTracks[taskId]
	if !exist {
		c.lockTracks.Unlock()
		reply.Err = ErrTaskNotFound
		return nil
	}
	if taskTrack.workerId != args.Whoami {
		c.lockTracks.Unlock()
		reply.Err = ErrUnmatchedWorker
		return nil
	}

	delete(c.taskTracks, taskId)
	c.lockTracks.Unlock()

	task := taskTrack.task
	if task.TaskType == MapTask {
		input := &c.mapInputs[task.WorkId]
		if input.asso_task != FILE_FINISHED {
			input.lock.Lock()
			if input.asso_task != FILE_FINISHED {
				input.asso_task = FILE_FINISHED
				for i, file := range args.Results {
					c.intermediates[i].lock.Lock()
					c.intermediates[i].value = append(c.intermediates[i].value, file)
					c.intermediates[i].lock.Unlock()
				}
				atomic.AddUint64(&c.nMapResult, 1)
			}
			input.lock.Unlock()
		}
	} else {
		input := &c.intermediates[task.WorkId]
		if input.asso_task != FILE_FINISHED {
			input.lock.Lock()
			if input.asso_task != FILE_FINISHED {
				input.asso_task = FILE_FINISHED
				atomic.AddUint64(&c.nReduceResult, 1)
			}
			input.lock.Unlock()
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return atomic.LoadUint64(&c.nReduceResult) == uint64(c.nReduce)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nFiles := len(files)
	c := Coordinator{
		nReduce:       nReduce,
		taskIdCnt:     1,
		nFile:         nFiles,
		taskTracks:    make(map[uint64]TaskTrack),
		mapInputs:     make([]Input, nFiles),
		intermediates: make([]Input, nReduce),
	}
	for i := range c.mapInputs {
		c.mapInputs[i].value = []string{files[i]}
	}
	c.server()
	return &c
}
