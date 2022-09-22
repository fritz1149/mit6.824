package coordinator

import "container/list"
import "time"

type task struct {
	filename string
	timer *time.Timer
}

type WorkScheduler struct {
	unassigned *list.List
	inprogress map[int]task
	unfinishedN int
	coordinator *Coordinator
	// finished *list.List
}

var aLongTime int = 10 //kill the worker having worked for more than 10s

func (ws *WorkScheduler) ResetTasks(inputfiles []string) *WorkScheduler{
	ws.unassigned = list.New()
	for _, filename := range inputfiles {
		ws.unassigned.PushBack(filename)
	}

	ws.inprogress = make(map[int]task)
	ws.unfinishedN = len(inputfiles)
	// ws.finished = list.New()
	return ws
}

func (ws *WorkScheduler) AllocateTask(workerIndex int) (string, bool) {
	if ws.unassigned.Len() == 0 {
		return "", false
	}
	ret := ws.unassigned.Front().Value.(string)
	ws.unassigned.Remove(ws.unassigned.Front())
	ws.inprogress[workerIndex] = task{
		filename: ret,
		timer: time.AfterFunc(
			time.Duration(aLongTime) * time.Second, 
			func(){ws.coordinator.WorkerDie(workerIndex)}),
	}
	return ret, true
}

func (ws *WorkScheduler) FinishTask(workerIndex int) {
	if task, ok := ws.inprogress[workerIndex]; ok {
		task.timer.Stop()
		delete(ws.inprogress, workerIndex)
		ws.unfinishedN--
	}
}

func (ws *WorkScheduler) AbandonTask(workerIndex int) {
	if task, ok := ws.inprogress[workerIndex]; ok {
		delete(ws.inprogress, workerIndex)
		ws.unassigned.PushBack(task.filename)
	}
}

func (ws *WorkScheduler) Done() bool {
	return ws.unfinishedN == 0
}

