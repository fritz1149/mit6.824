package coordinator

// import (
// 	"container/list"
// 	"fmt"
// 	"log"
// 	. "mr"
// )
import (
	"fmt"
	"sync"

	"6.824/myfile"
)

var (
	mapping  int = 0
	reducing int = 1
	done int = 2
)

type Coordinator struct {
	// Your definitions here.
	overallState *CriticalResource[int]
	reduceN int
	workScheduler *WorkScheduler
	workerManager *WorkerManager
	mux sync.Mutex
}

//
// start a thread that listens for RPCs from worker.go
//

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.overallState.Get() == done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.overallState = &CriticalResource[int]{}
	c.PrepareToMap(files, nReduce)
	c.server()
	return &c
}

func (c *Coordinator) PrepareToMap(files []string, nReduce int) {
	c.overallState.Set(mapping)
	c.workScheduler = (&WorkScheduler{coordinator: c}).ResetTasks(files)
	c.reduceN = nReduce
	c.workerManager = (&WorkerManager{}).ResetWorkers()
}

func (c *Coordinator) PrepareToReduce() {
	c.overallState.Set(reducing)
	files := make([]string, 0)
	for i := 0; i < c.reduceN; i++ {
		filename := fmt.Sprintf("mr-intermediate-%d.out", i)
		if myfile.FileExists(filename) {
			files = append(files, filename)
		}
	}
	c.workScheduler.ResetTasks(files)
}

func (c *Coordinator) PrepareToDone() {
	c.overallState.Set(done)
}

func (c *Coordinator) WorkerDie(workerIndex int) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.workScheduler.AbandonTask(workerIndex)
	c.workerManager.KillWorker(workerIndex)
}
