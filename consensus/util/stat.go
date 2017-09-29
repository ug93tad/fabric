package util

import (
	"sync"
	"time"
)

// Stat is the singleton that collects timing metrics
type Stat struct {
	txQueueTime        map[string]time.Time
	lockTxQueue        sync.Mutex
	batchConsensus     map[string]time.Time
	lockBatchConsensus sync.Mutex
	executeQueueTime   map[uint64]time.Time
	lockExecuteQueue   sync.Mutex
	batchExecute       map[uint64]time.Time
	lockBatchExecute   sync.Mutex
	commitQueueTime    map[uint64]time.Time
	lockCommitQueue    sync.Mutex
	batchCommit        map[uint64]time.Time
	lockBatchCommit    sync.Mutex

	reqCounter     uint64
	sampleInterval uint64
}

var statSyncOnce sync.Once
var stat *Stat

func GetStat() *Stat {
	statSyncOnce.Do(func() {
		stat = new(Stat)
		stat.txQueueTime = make(map[string]time.Time)
		stat.batchConsensus = make(map[string]time.Time)
		stat.batchExecute = make(map[uint64]time.Time)
		stat.batchCommit = make(map[uint64]time.Time)

		stat.executeQueueTime = make(map[uint64]time.Time)
		stat.commitQueueTime = make(map[uint64]time.Time)

		stat.reqCounter = 0
		stat.sampleInterval = 0xff
	})
	return stat
}

func (stat *Stat) SampleRPCRequest() bool {
	stat.reqCounter++
	//return (stat.reqCounter & stat.sampleInterval) == 0
	return true
}

func (stat *Stat) StartTx(txid string) {
	stat.lockTxQueue.Lock()
	defer stat.lockTxQueue.Unlock()
	stat.txQueueTime[txid] = time.Now()
}

func (stat *Stat) StartBatchConsensus(digest string) {
	stat.lockBatchConsensus.Lock()
	defer stat.lockBatchConsensus.Unlock()

	stat.batchConsensus[digest] = time.Now()
}

func (stat *Stat) StartExecutionQueue(seqNo uint64) {
	stat.lockExecuteQueue.Lock()
	defer stat.lockExecuteQueue.Unlock()

	stat.executeQueueTime[seqNo] = time.Now()
}

func (stat *Stat) GetExecutionQueueTime(seqNo uint64) (uint64, bool) {
	stat.lockExecuteQueue.Lock()
	defer stat.lockExecuteQueue.Unlock()
	x, ok := stat.executeQueueTime[seqNo]
	if !ok {
		return 0, ok
	} else {
		delete(stat.executeQueueTime, seqNo)
		return uint64(time.Since(x)), ok
	}
}

func (stat *Stat) StartCommitQueue(seqNo uint64) {
	stat.lockCommitQueue.Lock()
	defer stat.lockCommitQueue.Unlock()
	stat.commitQueueTime[seqNo] = time.Now()
}

func (stat *Stat) GetCommitQueueTime(seqNo uint64) (uint64, bool) {
	stat.lockCommitQueue.Lock()
	defer stat.lockCommitQueue.Unlock()
	x, ok := stat.commitQueueTime[seqNo]
	if !ok {
		return 0, ok
	} else {
		delete(stat.commitQueueTime, seqNo)
		return uint64(time.Since(x)), ok
	}
}

func (stat *Stat) StartBatchExecute(seqNo uint64) {
	stat.lockBatchExecute.Lock()
	defer stat.lockBatchExecute.Unlock()
	stat.batchExecute[seqNo] = time.Now()
}

func (stat *Stat) StartBatchCommit(seqNo uint64) {
	stat.lockBatchCommit.Lock()
	defer stat.lockBatchCommit.Unlock()
	stat.batchCommit[seqNo] = time.Now()
}

func (stat *Stat) GetTxQueueTime(txid string) (uint64, bool) {
	stat.lockBatchCommit.Lock()
	defer stat.lockBatchCommit.Unlock()
	x, ok := stat.txQueueTime[txid]
	if !ok {
		return 0, ok
	} else {
		delete(stat.txQueueTime, txid)
		return uint64(time.Since(x)), ok
	}
}

func (stat *Stat) GetBatchConsensusTime(digest string) (uint64, bool) {
	stat.lockBatchConsensus.Lock()
	defer stat.lockBatchConsensus.Unlock()

	x, ok := stat.batchConsensus[digest]
	if !ok {
		return 0, ok
	} else {
		delete(stat.batchConsensus, digest)
		return uint64(time.Since(x)), ok
	}
}

func (stat *Stat) GetBatchExecuteTime(seqNo uint64) (uint64, bool) {
	stat.lockBatchExecute.Lock()
	defer stat.lockBatchExecute.Unlock()

	x, ok := stat.batchExecute[seqNo]
	if !ok {
		return 0, ok
	} else {
		delete(stat.batchExecute, seqNo)
		return uint64(time.Since(x)), ok
	}
}

func (stat *Stat) GetBatchCommitTime(seqNo uint64) (uint64, bool) {
	stat.lockBatchCommit.Lock()
	defer stat.lockBatchCommit.Unlock()

	x, ok := stat.batchCommit[seqNo]
	if !ok {
		return 0, ok
	} else {
		delete(stat.batchCommit, seqNo)
		return uint64(time.Since(x)), ok
	}
}
