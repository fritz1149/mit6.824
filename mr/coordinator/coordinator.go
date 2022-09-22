package coordinator

// import (
// 	"container/list"
// 	"fmt"
// 	"log"
// 	. "mr"
// )
import "fmt"

var (
	mapping  int = 0
	reducing int = 1
	done int = 2
)

type Coordinator struct {
	// Your definitions here.
	overallState int
	reduceN int
	workScheduler *WorkScheduler
	workerManager *WorkerManager
}

//
// start a thread that listens for RPCs from worker.go
//

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.overallState == done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.PrepareToMap(files, nReduce)
	c.server()
	return &c
}

func (c *Coordinator) PrepareToMap(files []string, nReduce int) {
	c.overallState = mapping
	c.workScheduler = (&WorkScheduler{coordinator: c}).ResetTasks(files)
	c.reduceN = nReduce
	c.workerManager = new(WorkerManager).ResetWorkers()
}

func (c *Coordinator) PrepareToReduce() {
	c.overallState = reducing
	files := make([]string, c.reduceN)
	for i := 0; i < c.reduceN; i++ {
		files[i] = fmt.Sprintf("intermediate-%d.out", i)
	}
	c.workScheduler.ResetTasks(files)
}

func (c *Coordinator) PrepareToDone() {
	c.overallState = done
}

func (c *Coordinator) WorkerDie(workerIndex int) {
	c.workScheduler.AbandonTask(workerIndex)
	c.workerManager.KillWorker(workerIndex)
}
