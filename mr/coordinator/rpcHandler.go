package coordinator

import (
	"errors"
	// "fmt"
	"log"
	. "mr"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

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
	reply.WorkerIndex = c.workerManager.RegisterWorker()
	reply.ReduceN = c.reduceN
	return nil
}

func (c *Coordinator) WorkerRequest(args *RequestArgs, reply *RequestReply) error {
	if !c.workerManager.IsValidRPC(args.WorkerIndex, args.RPCIndex) {
		return errors.New("invalid workerIndex or rpcIndex")
	}

	filename, ok := c.workScheduler.AllocateTask(args.WorkerIndex)
	if !ok {
		reply.Filename = ""
		if c.overallState == done {
			reply.TaskState = Done
		} else {
			reply.TaskState = Idle
		}
		c.workerManager.FreeWorker(args.WorkerIndex)
	} else{
		reply.Filename = filename
		if c.overallState == mapping {
			reply.TaskState = Map
		} else {
			reply.TaskState = Reduce
		}
		c.workerManager.UseWorker(args.WorkerIndex)
	}
	
	return nil
}

func (c *Coordinator) WorkerFinish(args *RequestArgs, reply *RequestReply) error {
	if !c.workerManager.IsValidRPC(args.WorkerIndex, args.RPCIndex) {
		return errors.New("invalid workerIndex or rpcIndex")
	}

	c.workScheduler.FinishTask(args.WorkerIndex)
	if c.workScheduler.Done() {
		if c.overallState == mapping {
			c.PrepareToReduce()	
		} else if c.overallState == reducing {
			c.PrepareToDone()
		}
	}

	return c.WorkerRequest(args, reply)
}