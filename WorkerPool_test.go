package WorkerPool

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// WorkPackage todo
type WorkPackage struct {
	jobID  int
	f      float64
	div    float64
	result time.Duration
}

func (w *WorkPackage) Id() string {
	return strconv.Itoa(w.jobID)
}

func (w *WorkPackage) Run() error {
	startTime := time.Now()
	fmt.Println("Working...")
	// simulate cpu intense calculation
	f := w.f
	for f > 1 {
		f /= w.div
	}
	// simulate a result to be stored in the struct
	w.result = time.Since(startTime)
	return nil
}

func TestNewWorkerPool(t *testing.T) {
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
}

func TestStop(t *testing.T) {
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Stop()
	assert.EqualValues(t, 0, pool.workersRunning)
}

func TestClose(t *testing.T) {
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 0, pool.workersRunning)
}

func TestGetFinished(t *testing.T) {
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	job, done := pool.GetFinished()
	assert.False(t, done)
	assert.Nil(t, job)
}

func TestGetFinishedWait(t *testing.T) {
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	go func() {
		time.Sleep(2 * time.Second)
		fmt.Printf("Stopping worker pool\n")
		pool.Stop()
	}()
	job, done := pool.GetFinishedWait()
	assert.True(t, done)
	assert.Nil(t, job)
}

func TestQueueOne(t *testing.T) {
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	job := &WorkPackage{
		jobID:  1,
		f:      10000000.0,
		div:    1.0000001,
		result: 0,
	}
	err := pool.QueueJob(job)
	if err != nil {
		log.Println("could not add job")
	}
	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 1, pool.FinishedJobs())
}

func TestQueueMany(t *testing.T) {
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	for i := 1; i <= bufferSize; i++ {
		job := &WorkPackage{
			jobID:  i,
			f:      10000000.0,
			div:    1.0000001,
			result: 0,
		}
		err := pool.QueueJob(job)
		if err != nil {
			log.Println("could not add job")
		}
	}
	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, bufferSize, pool.FinishedJobs())
}

func TestWorkerPool_GetFinished(t *testing.T) {
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	for i := 1; i <= bufferSize; i++ {
		job := &WorkPackage{
			jobID:  i,
			f:      10000000.0,
			div:    1.0000001,
			result: 0,
		}
		err := pool.QueueJob(job)
		if err != nil {
			log.Println("could not add job")
		}
	}
	assert.EqualValues(t, bufferSize, pool.OpenJobs()+pool.InProgress()+pool.FinishedJobs())

	count := 0
	done := false
	var job Job
	for pool.HasJobs() {
		job, done = pool.GetFinished()
		if job != nil {
			fmt.Printf("Result: %s\n", job.(*WorkPackage).result)
			count++
		}
		if done {
			break
		}
	}

	pool.Close()
	assert.EqualValues(t, bufferSize, count)
}

func TestStressTest(t *testing.T) {
	for i := 0; i < 1000; i++ {
		t.Run("Stress", TestWorkerPool_GetFinished)
	}
}
