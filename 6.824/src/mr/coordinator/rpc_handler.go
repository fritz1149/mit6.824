package coordinator

import (
	"errors"
	// "fmt"
	"log"
	. "6.824/mr"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)
var inFinish bool = false
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := CoordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) WorkerRegister(args *RegisterArgs, reply *RegisterReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	reply.WorkerIndex = c.workerManager.RegisterWorker()
	reply.ReduceN = c.reduceN
	log.Printf("Coordinator: a worker get registered: %d\n", reply.WorkerIndex)
	return nil
}

func (c *Coordinator) WorkerRequest(args *RequestArgs, reply *RequestReply) error {
	if !args.IsFinish{
		c.mux.Lock()
		defer c.mux.Unlock()
	}
	if !c.workerManager.IsValidRPC(args.WorkerIndex, args.RPCIndex) {
		return errors.New("invalid workerIndex or rpcIndex")
	}
	if !c.workerManager.ShouldBeAllocated(args.WorkerIndex) {
		return errors.New("worker is busy, should not be allocated some work")
	}
	filename, ok := c.workScheduler.AllocateTask(args.WorkerIndex)
	log.Printf("Coordinator: a worker request: %d, overallState is %d, input filename is %s, ok is %s\n", 
				args.WorkerIndex, c.overallState.Get(), filename, strconv.FormatBool(ok))
	if !ok {
		reply.Filename = ""
		if c.overallState.Get() == done {
			reply.TaskState = Done
		} else {
			reply.TaskState = Idle
		}
		c.workerManager.FreeWorker(args.WorkerIndex)
	} else{
		reply.Filename = filename
		if c.overallState.Get() == mapping {
			reply.TaskState = Map
		} else {
			reply.TaskState = Reduce
		}
		c.workerManager.UseWorker(args.WorkerIndex)
	}
	
	return nil
}

func (c *Coordinator) WorkerFinish(args *RequestArgs, reply *RequestReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if !c.workerManager.IsValidRPC(args.WorkerIndex, args.RPCIndex) {
		return errors.New("invalid workerIndex or rpcIndex")
	}

	log.Printf("Coordinator: a worker finish its work: %d\n", args.WorkerIndex)
	c.workScheduler.FinishTask(args.WorkerIndex)
	c.workerManager.FreeWorker(args.WorkerIndex)
	if c.workScheduler.Done() {
		if c.overallState.Get() == mapping {
			c.PrepareToReduce()	
		} else if c.overallState.Get() == reducing {
			c.PrepareToDone()
		}
	}
	return c.WorkerRequest(args, reply)
}