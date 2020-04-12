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

// Stress tests
func TestStressTest(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestStop)
		t.Run("Stress", TestDoubleStop)
		t.Run("Stress", TestClose)
		t.Run("Stress", TestDoubleClose)
		t.Run("Stress", TestShutdown)
		t.Run("Stress", TestGetFinished)
		t.Run("Stress", TestGetFinishedWait)
		t.Run("Stress", TestQueueOne)
		t.Run("Stress", TestQueueMany)
	}
}

// Create a pool and test that the workers are running
func TestNewWorkerPool(t *testing.T) {
	t.Parallel()
	noOfWorkers := 4
	bufferSize := 1
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
}

// Stop an empty pool, test that the workers hav e been stopped
// and try to enqueue work after stopping
func TestStop(t *testing.T) {
	t.Parallel()
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Stop()
	assert.EqualValues(t, 0, pool.workersRunning)
	err := pool.QueueJob(nil)
	if err != nil {
		log.Println("Queue has been closed")
	}
	assert.NotNil(t, err)
}

// Call stop twice and make sure this does not panic (b/o
// closed channel
func TestDoubleStop(t *testing.T) {
	t.Parallel()
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Stop()
	assert.EqualValues(t, 0, pool.workersRunning)
	err := pool.QueueJob(nil)
	if err != nil {
		log.Println("Queue has been closed")
	}
	assert.NotNil(t, err)
	assert.NotPanics(t, func() {
		pool.Stop()
	})
}

func TestClose(t *testing.T) {
	t.Parallel()
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 0, pool.workersRunning)

	err := pool.QueueJob(nil)
	if err != nil {
		log.Println("Queue has been closed")
	}
	assert.NotNil(t, err)
}

func TestDoubleClose(t *testing.T) {
	t.Parallel()
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Close()
	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 0, pool.workersRunning)
}

func TestShutdown(t *testing.T) {
	t.Parallel()
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Shutdown()
	job, done := pool.GetFinished()
	assert.True(t, done)
	assert.Nil(t, job)
}

func TestGetFinished(t *testing.T) {
	t.Parallel()
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	job, done := pool.GetFinished()
	assert.False(t, done)
	assert.Nil(t, job)
}

func TestGetFinishedWait(t *testing.T) {
	t.Parallel()
	noOfWorkers := 4
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	go func() {
		time.Sleep(1 * time.Second)
		fmt.Printf("Stopping worker pool\n")
		pool.Shutdown()
	}()
	job, done := pool.GetFinishedWait()
	assert.True(t, done)
	assert.Nil(t, job)
}

func TestQueueOne(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
