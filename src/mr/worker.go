package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "path/filepath"
import "bufio"
import "strings"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
func (a ByKey) Len() int 			{ return len(a) }
func (a ByKey) Swap(i, j int)		{ a[i], a[j] = a[j], a[i]}
func (a ByKey) Less(i, j int) bool	{ return a[i].Key < a[j].Key}
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
	workerId := registerWithCoordinator()
	lastActivate := time.Now()
	for {
		reply := callTask(workerId)
		// fmt.Println("Get a task")
		if reply.TaskDesc == MapTask {
			ok := MapTaskExecutor(reply, mapf)
			if ok != nil {
				return 
			}
		}
		if reply.TaskDesc == ReduceTask {
			ok := ReduceTaskExecutor(reply, reducef)
			if ok != nil {
				return 
			}
		}
		callDone(reply, workerId)
		if time.Since(lastActivate) > 10 * time.Second{
			return 
		} 
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
}
func MapTaskExecutor(info TaskReply, mapf func(string, string) []KeyValue) error {

	content, err := os.ReadFile(info.InputFile)

	if err != nil {
		return err
	}
	kva := mapf(info.InputFile, string(content))

	Files := make([]*os.File, info.ReduceN)
	Names := make([]string, info.ReduceN)
	for i := 0; i < info.ReduceN; i++ {
		err := os.MkdirAll("temp", 0755)
		if err != nil {
			return err
		}
		file , err := os.Create(filepath.Join("temp", fmt.Sprintf("mr-itermidiate-%d-%d.txt",info.TaskId, i)))
		if err != nil {
			for _, name := range Names {
				os.Remove(name)
			}
			return err
		}
		Files[i] = file
		Names[i] = file.Name()
	}
	defer func() {
		for _, file := range Files {
			if file != nil {
				file.Close()
			}
		}
		if err != nil {
			for _, name := range Names {
				os.Remove(name)
			}
		}
	}()

	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % info.ReduceN
		fmt.Fprintf(Files[reduceIndex], "%v %v\n",kv.Key, kv.Value)
	}
	return nil
}
func collectIntermidateFiles(reduceIndex, nMap int) []*os.File {
	Files := make([]*os.File, nMap)

	for i := 0; i < nMap; i++ {
		file, err := os.OpenFile(filepath.Join("temp", fmt.Sprintf("mr-itermidiate-%d-%d.txt", i, reduceIndex)), os.O_RDONLY, 0644)
		if err != nil {
			for j := 0; j < i; j++ {
				Files[j].Close()
			}
		}
		Files[i] = file
	}
	return Files

}

func ReduceTaskExecutor(info TaskReply, reducef func(string, []string) string) error {

	Files := collectIntermidateFiles(info.ReduceIndex, info.MapN)
	
	outputFile, err := os.Create(fmt.Sprintf("mr-out-%d", info.ReduceIndex))
	if err != nil {
		return err
	}
	outputName := outputFile.Name()
	defer func() {
		outputFile.Close()
		if err != nil {
			os.Remove(outputName)
		}
		for _, file := range Files {
			file.Close()
		}
	}()
	var kva []KeyValue
	for _, file := range Files {
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, " ", 2)
			if len(parts) == 2 {
				kv := KeyValue {
					Key: 	parts[0],
					Value: 	parts[1],
				}
				kva = append(kva, kv)
			}
		}
		
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	sort.Sort(ByKey(kva))

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)
		
		i = j
	}

	return nil
}
func registerWithCoordinator() int {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	// fmt.Println("begins to call")
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		log.Fatalf("Failed to register worker")
	}

	return reply.WorkerId
}

func callTask(workerId int) TaskReply{
	args := TaskArgs{
		WorkerId : workerId,
	}
	reply := TaskReply{}

	ok := call("Coordinator.RequireTask", &args, &reply)
	if !ok {
		log.Fatalf("Failed to require task from coordinator.")
	}
	return reply
}

func callDone(info TaskReply, workerId int) {
	args := TaskCompleteArgs {
		TaskId : info.TaskId,
		TaskDesc : info.TaskDesc,
		WorkerId : workerId,
	}
	reply := TaskCompleteReply{}

	ok := call("Coordinator.DoneTask", &args, &reply)

	if !ok {
		log.Fatalf("Failed to mark success.")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
