package WorkerPool

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync/atomic"
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
	if debug {
		fmt.Println("Working...")
	}
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
	}
}

// Create a pool and test that the workers are running
func TestNewWorkerPool(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 1
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
}

// Stop an empty pool, test that the workers have been stopped
// and try to enqueue work after stopping
func TestStop(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Stop()
	assert.EqualValues(t, 0, pool.workersRunning)

	err := pool.QueueJob(nil)
	if err != nil {
		if debug {
			log.Println("Queue has been closed")
		}
	}
	assert.NotNil(t, err)
}

// Call stop twice and make sure this does not panic (b/o
// closed channel
func TestDoubleStop(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Stop()
	assert.EqualValues(t, 0, pool.workersRunning)
	err := pool.QueueJob(nil)
	if err != nil {
		if debug {
			log.Println("Queue has been closed")
		}
	}
	assert.NotNil(t, err)
	assert.NotPanics(t, func() {
		pool.Stop()
	})
}

// Close an empty pool and wait until all workers have stopped. Try
// to add a job and check that it fails
func TestClose(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 0, pool.workersRunning)

	err := pool.QueueJob(nil)
	if err != nil {
		if debug {
			log.Println("Queue has been closed")
		}
	}
	assert.NotNil(t, err)
}

// Close and empty pool twice and check the double closing does not panic.
func TestDoubleClose(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Close()
	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 0, pool.workersRunning)
}

// Shutdown empty pool and try to get finished job. Test
// that done is is true and signaling that the finished queue
// is closed
func TestShutdown(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	pool.Shutdown()
	job, done := pool.GetFinished()
	assert.True(t, done)
	assert.Nil(t, job)
}

// Try to retrieve finished jobs from empty pool. Check
// that job is empty but done is false to signal that finished
// queue is still open.
func TestGetFinished(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	job, done := pool.GetFinished()
	assert.False(t, done)
	assert.Nil(t, job)
}

// Try to retrieve finished jobs from empty pool. Check that
// call has blocked until timeout kicked in. Also check
// that finished queue is now closed (done=true)
func TestGetFinishedWait(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	timeout := false
	go func() {
		time.Sleep(1 * time.Second)
		if debug {
			fmt.Printf("Stopping worker pool\n")
		}
		timeout = true
		pool.Shutdown()
	}()
	job, done := pool.GetFinishedWait()
	assert.True(t, timeout)
	assert.True(t, done)
	assert.Nil(t, job)
}

// Create one WorkPackage (Job) and add/enqueue it to the pool.
// Close, wait and check that there is a job in the finished queue
func TestQueueOne(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	job := &WorkPackage{
		jobID:  1,
		f:      10000000.0,
		div:    1.0000001,
		result: 0,
	}
	err := pool.QueueJob(job)
	if err != nil {
		if debug {
			log.Println("could not add job")
		}
	}
	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 1, pool.FinishedJobs())
}

// Stress tests
func _TestStressQueueMany(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestQueueMany)
	}
}

// Create several WorkPackages (Job) and add/enqueue them to the pool.
// Close, wait and check that there is a job in the finished queue
func TestQueueMany(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
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
			if debug {
				log.Println("could not add job")
			}
		}
	}
	pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, bufferSize, pool.FinishedJobs())
}

// Create several WorkPackages (Job) and add/enqueue them to the pool.
// Retrieve finished jobs and compare number of jobs enqueued
// and retrieved.
func TestWorkerPool_GetFinished(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU()*2 - 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
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
			if debug {
				log.Println("could not add job")
			}
		}
	}
	count := 0
	done := false
	var job Job
	for pool.HasJobs() {
		job, done = pool.GetFinished()
		if job != nil {
			if debug {
				fmt.Printf("Result: %s\n", job.(*WorkPackage).result)
			}
			count++
		}
		if done {
			break
		}
	}
	pool.Close()
	assert.EqualValues(t, bufferSize, count)
}

func _TestStressWorkerPool_Consumer(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestWorkerPool_Consumer)
	}
}

// This uses a separate consumer thread to read results
// and a timer to close the WorkerPool.
// Producer is much faster.
// When closed number of enqueued jobs need to match
// the retrieved number of jobs.
func TestWorkerPool_Consumer(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 100
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	go func() {
		time.Sleep(5 * time.Second)
		pool.Close()
	}()

	produced := 0
	consumed := 0

	// consumer
	go func() {
		for pool.WorkersActive() || pool.FinishedJobs() > 0 {
			job, closed := pool.GetFinished()
			if closed {
				break
			}
			if job != nil {
				if debug {
					fmt.Printf("Result: %s\n", job.(*WorkPackage).result)
				}
				consumed++
			}
			// busy wait therefore give other routines a chance
			runtime.Gosched()
		}
		assert.EqualValues(t, produced, consumed)
	}()

	// producer
	for {
		job := &WorkPackage{
			jobID:  produced,
			f:      1000000.0,
			div:    1.000001,
			result: 0,
		}
		if debug {
			fmt.Printf("Add job: %d\n", produced)
		}
		err := pool.QueueJob(job)
		if err != nil {
			if debug {
				log.Println("could not add job")
			}
			break
		}
		produced++
	}

	pool.Close()
	pool.waitGroup.Wait()
}

func _TestStressWorkerPool_Loop2(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestWorkerPool_Loop2)
	}
}

// This uses a separate consumer  and producer thread to
// create and retrieve jobs. Producer is much slower
func TestWorkerPool_Loop2(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 100
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	go func() {
		time.Sleep(5 * time.Second)
		pool.Close()
	}()

	consumed := 0
	produced := 0

	// consumer
	go func() {
		for pool.WorkersActive() || pool.FinishedJobs() > 0 {
			job, closed := pool.GetFinished()
			if closed {
				break
			}
			if job != nil {
				if debug {
					fmt.Printf("Result: %s\n", job.(*WorkPackage).result)
				}
				consumed++
			}
			// busy wait therefore give other routines a chance
			runtime.Gosched()
		}
		assert.EqualValues(t, produced, consumed)
		assert.EqualValues(t, 0, len(pool.jobs))
	}()

	// producer
	for {
		time.Sleep(100 * time.Millisecond)
		job := &WorkPackage{
			jobID:  produced,
			f:      1000000.0,
			div:    1.000001,
			result: 0,
		}
		if debug {
			fmt.Printf("Add job: %d\n", produced)
		}
		err := pool.QueueJob(job)
		if err != nil {
			if debug {
				log.Println("could not add job")
			}
			break
		}
		produced++
	}

	pool.Close()
	pool.waitGroup.Wait()
}

func _TestStressWorkerPool_Two(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestWorkerPool_Two)
	}
}

// This uses two separate consumer threads to read results
// and a timer to close the WorkerPool.
// It also uses two producers
func TestWorkerPool_Two(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 100
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	go func() {
		time.Sleep(5 * time.Second)
		pool.Close()
	}()

	consumed := int32(0)
	produced := int32(0)

	// consumer 1
	go func() {
		for pool.WorkersActive() || pool.FinishedJobs() > 0 {
			job, closed := pool.GetFinished()
			if closed {
				break
			}
			if job != nil {
				if debug {
					fmt.Printf("Result: %s\n", job.(*WorkPackage).result)
				}
				atomic.AddInt32(&consumed, 1)
			}
			// busy wait therefore give other routines a chance
			runtime.Gosched()
		}
	}()

	// consumer 2
	go func() {
		for pool.WorkersActive() || pool.FinishedJobs() > 0 {
			job, closed := pool.GetFinished()
			if closed {
				break
			}
			if job != nil {
				if debug {
					fmt.Printf("Result: %s\n", job.(*WorkPackage).result)
				}
				atomic.AddInt32(&consumed, 1)
			}
			// busy wait therefore give other routines a chance
			runtime.Gosched()
		}
	}()

	// producer 1
	for {
		time.Sleep(100 * time.Millisecond)
		job := &WorkPackage{
			jobID:  int(atomic.LoadInt32(&produced)),
			f:      1000000.0,
			div:    1.000001,
			result: 0,
		}
		if debug {
			fmt.Printf("P1 add job: %d\n", produced)
		}
		err := pool.QueueJob(job)
		if err != nil {
			if debug {
				log.Println("could not add job")
			}
			break
		}
		atomic.AddInt32(&produced, 1)
	}

	// producer 2
	for {
		time.Sleep(100 * time.Millisecond)
		job := &WorkPackage{
			jobID:  int(atomic.LoadInt32(&produced)),
			f:      1000000.0,
			div:    1.000001,
			result: 0,
		}
		if debug {
			fmt.Printf("P2 add job: %d\n", produced)
		}
		err := pool.QueueJob(job)
		if err != nil {
			if debug {
				log.Println("could not add job")
			}
			break
		}
		atomic.AddInt32(&produced, 1)
	}

	pool.Close()
	pool.waitGroup.Wait()
	for pool.WorkersActive() || pool.FinishedJobs() > 0 {
	}

	assert.EqualValues(t, produced, consumed)
	assert.EqualValues(t, 0, len(pool.jobs))
}

// Two producers. Finished jobs are ignored.
func TestWorkerPool_ProduceOnly(t *testing.T) {
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 1000
	pool := NewWorkerPool(noOfWorkers, bufferSize, false)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	go func() {
		time.Sleep(30 * time.Second)
		pool.Close()
	}()

	i := int32(0)
	// producer 1
	go func() {
		for {
			atomic.AddInt32(&i, 1)
			job := &WorkPackage{
				jobID:  int(i),
				f:      10000000.0,
				div:    1.0000001,
				result: 0,
			}
			fmt.Printf("P1 adds job: %d\n", i)
			err := pool.QueueJob(job)
			if err != nil {
				log.Println("could not add job")
				break
			}
		}
	}()

	// producer 2
	go func() {
		for {
			atomic.AddInt32(&i, 1)
			job := &WorkPackage{
				jobID:  int(i),
				f:      1000000.0,
				div:    1.000001,
				result: 0,
			}
			fmt.Printf("P2 adds job: %d\n", i)
			err := pool.QueueJob(job)
			if err != nil {
				log.Println("could not add job")
				break
			}
		}
	}()

	pool.waitGroup.Wait()
}
