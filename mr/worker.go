package mr

import (
	"bufio"
	"container/list"
	"fmt"
	// "go/scanner"
	"hash/fnv"
	"io/ioutil"
	"log"
	"6.824/myfile"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker struct {
	workerIndex int
	reduceN int
	rpcIndex int
}
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapTask(filename string, reduceN int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))
	outBuffer := make(map[int]*list.List)
	outList := list.New()
	outBuffer[ihash(kva[0].Key) % reduceN] = outList
	outList.PushBack(kva[0])
	for i, kv := range kva[1:]{
		last := kva[i]
		if kv.Key != last.Key{
			outIndex := ihash(kv.Key) % reduceN
			_, ok := outBuffer[outIndex]
			if !ok {
				outList = list.New()
				outBuffer[outIndex] = outList
			}
		}
		outList.PushBack(kv)
	}

	for outIndex, outList := range outBuffer{
		file, err :=  myfile.OpenAnyway(fmt.Sprintf("intermediate-%d.out", outIndex))
		if err != nil {
			log.Fatal("OpenAnyway failed")
		}
		write := bufio.NewWriter(file)
		for iter := outList.Front(); iter != nil; iter = iter.Next() {
			kv := iter.Value.(KeyValue)
			write.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
		}
		write.Flush()
		file.Close()
	}
}

func reduceTask(filename string, reducef func(string, []string) string) {
	var start, end int
	for i := 0; i < len(filename); i++ {
		if filename[i] == '-' {
			start = i + 1
		} else if filename[i] == '.' {
			end = i
			break
		}
	}
	outIndex := filename[start : end]
	

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	key :=""
	values := make([]string, 0)
	output := make([]KeyValue, 0)
	for scanner.Scan() {
		var keyInThisLine, value string
		fmt.Sscanf(scanner.Text(), "%s %s", keyInThisLine, value)
		if key == "" {
			key = keyInThisLine
		}
		if keyInThisLine != key {
			output = append(output, KeyValue{Key: key, Value: reducef(key, values)})
			key = keyInThisLine
			values = make([]string, 0)
		}
		values = append(values, value)
	}
	if key != ""{
		output = append(output, KeyValue{Key: key, Value: reducef(key, values)})
	}
	file.Close()

	file, err = myfile.OpenAnyway(fmt.Sprintf("mr-out-%d.out", outIndex))
	if err != nil {
		log.Fatal("OpenAnyway failed")
	}
	write := bufio.NewWriter(file)
	for _, kv := range output{
		write.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
	}
	write.Flush()
	file.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	wk := &worker{rpcIndex: 0}
	wk.regiser()
	var sleepTime int = 2 //a worker sleeps for 2 second when there is no work to do
	
	for taskState, filename := wk.request(); taskState != Done; {
		if taskState == Idle {
			time.Sleep(time.Duration(sleepTime) * time.Second)
		} else if taskState == Map {
			mapTask(filename, wk.reduceN, mapf)
		} else if taskState == Reduce {
			reduceTask(filename, reducef)
		}
		taskState, filename = wk.request()
	}
}

func (wk *worker) regiser(){
	args := RegisterArgs{}
	reply := RegisterReply{}
	if ok := call("Coordinator.WorkerRegister", &args, &reply); !ok {
		log.Fatal("worker failed to register")
	}
	wk.workerIndex = reply.WorkerIndex
	wk.reduceN = reply.ReduceN
}

func (wk *worker) request() (int, string) {
	wk.rpcIndex++
	args := RequestArgs{WorkerIndex: wk.workerIndex, RPCIndex: wk.rpcIndex}
	reply := RequestReply{}
	if ok := call("Coordinator.WorkerRequest", &args, &reply); !ok {
		log.Fatal(fmt.Sprintf("worker %d failed to request", wk.workerIndex))
	}
	return reply.TaskState, reply.Filename
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
	sockname := CoordinatorSock()
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
