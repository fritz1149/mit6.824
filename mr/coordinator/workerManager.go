package coordinator

var(
	idle int = 0
	busy int = 1
	dead int = 2
)

type worker struct {
	state int
	rpcIndex int
}

type WorkerManager struct {
	workerList []worker
}

func (wm *WorkerManager) ResetWorkers() *WorkerManager {
	wm.workerList = make([]worker, 0)
	return wm
}

func (wm *WorkerManager) RegisterWorker() int {
	wm.workerList = append(wm.workerList, worker{idle, 0})
	return len(wm.workerList) - 1
}

func (wm *WorkerManager) IsValidRPC(workerIndex int, rpcIndex int) bool {
	if workerIndex < len(wm.workerList) && wm.workerList[workerIndex].rpcIndex < rpcIndex {
		wm.workerList[workerIndex].rpcIndex = rpcIndex
		return true
	}
	return false
}

func (wm *WorkerManager) UseWorker(workerIndex int) {
	wm.workerList[workerIndex].state = busy
}

func (wm *WorkerManager) FreeWorker(workerIndex int) {
	wm.workerList[workerIndex].state = idle
}

func (wm *WorkerManager) KillWorker(workerIndex int) {
	wm.workerList[workerIndex].state = dead
}