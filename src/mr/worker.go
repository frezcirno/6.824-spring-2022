package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"syscall"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type TaskType int

const (
	MapTask    TaskType = iota
	ReduceTask TaskType = iota
	RelaxTask  TaskType = iota
)

type Task struct {
	TaskId   uint64
	TaskType TaskType
	WorkId   int
	TaskArgs []string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var nReduce int

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func callWithRetry(rpcname string, args interface{}, reply interface{}) MapReduceError {
	for i := 0; i < 3; i++ {
		if call(rpcname, args, reply) {
			return MapReduceOk
		}
	}
	return ErrRpcFailed
}

func getParams(key string) (int, MapReduceError) {
	// declare an argument structure.
	args := GetParamsArgs{Key: key}

	// declare a reply structure.
	reply := GetParamsReply{}

	res := callWithRetry("Coordinator.GetParams", &args, &reply)
	if res != MapReduceOk {
		return -1, res
	}

	return reply.Value, MapReduceOk
}

func getTask(id uint64) (Task, MapReduceError) {
	// declare an argument structure.
	args := GetTaskArgs{Whoami: id}

	// declare a reply structure.
	reply := GetTaskReply{}

	res := callWithRetry("Coordinator.GetTask", &args, &reply)
	if res != MapReduceOk {
		return Task{}, res
	}

	return reply.Task, MapReduceOk
}

func submitTask(id uint64, taskId uint64, results []string) MapReduceError {
	// declare an argument structure.
	args := SubmitTaskArgs{Whoami: id, TaskId: taskId, Results: results}

	// declare a reply structure.
	reply := SubmitTaskReply{}

	res := callWithRetry("Coordinator.SubmitTask", &args, &reply)
	if res != MapReduceOk {
		return res
	}

	return reply.Err
}

func GenWorkerId() uint64 {
	return (uint64)(os.Getpid() + syscall.Gettid())
}

func doMapTask(mapf func(string, string) []KeyValue, file string, workId int) ([]string, error) {
	contents, err := os.ReadFile(file)
	if err != nil {
		log.Fatal("read file error:", err)
		return nil, err
	}
	mapResult := mapf(file, string(contents))

	buckets := make([]string, nReduce)
	for _, kv := range mapResult {
		buckets[ihash(kv.Key)%nReduce] += kv.Key + " " + kv.Value + "\n"
	}

	files := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		f, err := ioutil.TempFile("", "mr-tmp-")
		if err != nil {
			log.Fatal("create file error:", err)
			return nil, err
		}
		f.WriteString(buckets[i])
		f.Close()

		oname := fmt.Sprintf("mr-%d-%d", workId, i)
		os.Remove(oname)
		err = os.Rename(f.Name(), oname)
		if err != nil {
			log.Fatal("rename file error:", err)
			return nil, err
		}
		files[i] = oname
	}

	return files, nil
}

func doReduceTask(reducef func(string, []string) string, workId int, files []string) error {
	// read the files.
	kvs := make([]KeyValue, 0)
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatal("open file error:", err)
			return err
		}
		defer f.Close()

		kv := KeyValue{}
		for {
			_, err := fmt.Fscanf(f, "%v %v", &kv.Key, &kv.Value)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("scan file error:", err)
				return err
			}
			kvs = append(kvs, kv)
		}
	}

	// sort the kvs.
	sort.Sort(ByKey(kvs))

	// output the kvs to a file.
	f, err := ioutil.TempFile("", "mr-tmp-")
	if err != nil {
		log.Fatal("create temp file error:", err)
		return err
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	f.Close()

	// rename the file.
	oname := "mr-out-" + strconv.Itoa(workId)
	os.Remove(oname)
	err = os.Rename(f.Name(), oname)
	if err != nil {
		log.Fatal("rename file error:", err)
		return err
	}

	return nil
}

//
// main/mrworker.go calls this function.
// Mainloop of worker.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	id := GenWorkerId()

	_nReduce, err := getParams("nReduce")
	if err != MapReduceOk {
		log.Fatal("get params error:", err)
		return
	}
	nReduce = int(_nReduce)

	for {
		task, err := getTask(id)
		if err != MapReduceOk {
			log.Fatal("getTask failed:", err)
			break
		}
		switch task.TaskType {
		case MapTask:
			outfiles, err := doMapTask(mapf, task.TaskArgs[0], task.WorkId)
			if err != nil {
				log.Fatal("doMapTask failed:", err)
				break
			}
			err = submitTask(id, task.TaskId, outfiles)
			if err != MapReduceOk {
				log.Fatal("submitTask failed:", err)
			}
		case ReduceTask:
			err := doReduceTask(reducef, task.WorkId, task.TaskArgs)
			if err != nil {
				log.Fatal("doReduceTask failed:", err)
				break
			}
			err = submitTask(id, task.TaskId, nil)
			if err != MapReduceOk {
				log.Fatal("submitTask failed:", err)
			}
		case RelaxTask:
			// exit the worker.
			return
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
