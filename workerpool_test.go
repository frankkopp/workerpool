/*
 * MIT License
 *
 * Copyright (c) 2020 Frank Kopp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package workerpool

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// /////////////////////////////////////
// Test data

// WorkPackage for usage during unit testing
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
		fmt.Printf("Working on Job %d\n", w.jobID)
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

// Test data
// /////////////////////////////////////

// Stress tests
func TestStressTest(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestStop)
		t.Run("Stress", TestDoubleStop)
		t.Run("Stress", TestClose)
		t.Run("Stress", TestDoubleClose)
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

	if debug {
		fmt.Println("Waiting : ", pool.Jobs())
		fmt.Println("Working : ", pool.RunningJobs())
		fmt.Println("Finished: ", pool.FinishedJobs())
		fmt.Println("All     : ", pool.Jobs())
		fmt.Println("HasJobs : ", pool.HasJobs())
		fmt.Println("Active  : ", pool.Active())
	}
	assert.EqualValues(t, 0, pool.Jobs())
	assert.EqualValues(t, 0, pool.RunningJobs())
	assert.EqualValues(t, 0, pool.FinishedJobs())
	assert.EqualValues(t, 0, pool.Jobs())
	assert.True(t, pool.Active())
	assert.False(t, pool.HasJobs())
}

// Stop an empty pool, test that the workers have been stopped
// and try to enqueue work after stopping
func TestStop(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	_ = pool.Stop()
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
	_ = pool.Stop()
	assert.EqualValues(t, 0, pool.workersRunning)
	err := pool.QueueJob(nil)
	if err != nil {
		if debug {
			log.Println("Queue has been closed")
		}
	}
	assert.NotNil(t, err)
	assert.NotPanics(t, func() {
		_ = pool.Stop()
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

	_ = pool.Close()
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
	_ = pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 0, pool.workersRunning)
	assert.NotPanics(t, func() {
		_ = pool.Close()
	})
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
		_ = pool.Stop()
	}()
	job, done := pool.GetFinishedWait()
	assert.True(t, timeout)
	assert.True(t, done)
	assert.Nil(t, job)
}

// Add/enqueue a nil job to the pool.
func TestQueueNil(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU()
	bufferSize := 5
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	err := pool.QueueJob(nil)
	if err != nil {
		log.Println("could not add job")
	}
	_ = pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 0, pool.FinishedJobs())
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
		log.Println("could not add job")
	}
	_ = pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, 1, pool.FinishedJobs())
}

// Stress tests
func TestStressQueueMany(t *testing.T) {
	t.SkipNow()
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
			f:      1000000.0,
			div:    1.000001,
			result: 0,
		}
		err := pool.QueueJob(job)
		if err != nil {
			if debug {
				log.Println("could not add job")
			}
		}
	}
	_ = pool.Close()
	pool.waitGroup.Wait()
	assert.EqualValues(t, bufferSize, pool.FinishedJobs())
}

func TestStressTestWorkerPoolGetFinished(t *testing.T) {
	t.SkipNow()
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestWorkerPoolGetFinished)
	}
}

// Create several WorkPackages (Job) and add/enqueue them to the pool.
// Retrieve finished jobs and compare number of jobs enqueued
// and retrieved.
func TestWorkerPoolGetFinished(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU()*2 - 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)
	for i := 1; i <= bufferSize; i++ {
		job := &WorkPackage{
			jobID:  i,
			f:      100000.0,
			div:    1.00001,
			result: 0,
		}
		err := pool.QueueJob(job)
		if err != nil {
			if debug {
				log.Println("could not add job")
			}
		}
	}

	_ = pool.Close()

	count := 0
	for {
		job, done := pool.GetFinished()
		if done {
			break
		}
		if job != nil {
			if debug {
				fmt.Printf("Result: %s\n", job.(*WorkPackage).result)
			}
			count++
		}
		runtime.Gosched()
	}

	assert.EqualValues(t, bufferSize, count)
}

func TestStressWorkerPoolConsumer(t *testing.T) {
	t.SkipNow()
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestCloseAndRetrieve)
	}
}

// This uses separate producer and consumer threads to
// queue jobs and retrieve finished jobs.
// When closed the number of retrieved jobs needs to be equal
// to the number of enqueued jobs as we execute all remaining
// remaining jobs with Close()
func TestCloseAndRetrieve(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU()
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	done := make(chan bool)

	go func() {
		time.Sleep(1 * time.Second)
		_ = pool.Close()
		if debug {
			fmt.Println("Stopping workers ===============")
		}
	}()

	consumed := int32(0)
	produced := int32(0)

	// producer 1
	go func() {
		for {
			job := &WorkPackage{
				jobID:  int(atomic.LoadInt32(&produced)),
				f:      1000000.0,
				div:    1.000001,
				result: 0,
			}
			err := pool.QueueJob(job)
			if err != nil {
				if debug {
					log.Printf("P1 could not add job %d\n", job.jobID)
				}
				break
			}
			if debug {
				fmt.Printf("P1 add job: %d\n", job.jobID)
			}
			atomic.AddInt32(&produced, 1)
		}
	}()

	// consumer 1
	go func() {
		for {
			job, closed := pool.GetFinishedWait()
			if closed {
				break
			}
			if job != nil {
				if debug {
					fmt.Printf("C1 Result for %s: %s\n", job.Id(), job.(*WorkPackage).result)
				}
				atomic.AddInt32(&consumed, 1)
			}
			// slow down the consumer to have many jobs in the waiting list
			// when stopping to prove that these are discarded.
			time.Sleep(50 * time.Millisecond)
		}
		done <- true
	}()

	pool.waitGroup.Wait()
	<-done

	if debug {
		fmt.Println("Produced: ", atomic.LoadInt32(&produced))
		fmt.Println("Consumed: ", atomic.LoadInt32(&consumed))
		fmt.Println("Waiting Queue : ", pool.WaitingJobs())
		fmt.Println("Finished Queue: ", pool.FinishedJobs())
		fmt.Println("Consumed: ", atomic.LoadInt32(&consumed))
	}
	assert.EqualValues(t, consumed, produced)
	assert.EqualValues(t, pool.WaitingJobs(), 0)
	assert.EqualValues(t, 0, pool.FinishedJobs())
}

// This uses separate producer and consumer threads to
// queue jobs and retrieve finished jobs.
// When stopped the number of retrieved jobs needs to be lower
// than the number of enqueued jobs as we omit any waiting
// jobs with Stop()
func TestStopAndRetrieve(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU()
	bufferSize := 500
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	done := make(chan bool)

	go func() {
		time.Sleep(2 * time.Second)
		_ = pool.Stop()
		if debug {
			fmt.Println("Stopping workers ===============")
		}
	}()

	consumed := int32(0)
	produced := int32(0)

	// producer 1
	go func() {
		for {
			job := &WorkPackage{
				jobID:  int(atomic.LoadInt32(&produced)),
				f:      10000000.0,
				div:    1.0000001,
				result: 0,
			}
			err := pool.QueueJob(job)
			if err != nil {
				if debug {
					log.Printf("P1 could not add job %d\n", job.jobID)
				}
				break
			}
			if debug {
				fmt.Printf("P1 add job: %d\n", job.jobID)
			}
			atomic.AddInt32(&produced, 1)
		}
	}()

	// consumer 1
	go func() {
		for {
			job, closed := pool.GetFinishedWait()
			if closed {
				break
			}
			if job != nil {
				if debug {
					fmt.Printf("C1 Result for %s: %s\n", job.Id(), job.(*WorkPackage).result)
				}
				atomic.AddInt32(&consumed, 1)
			}
			// slow down the consumer to have many jobs in the waiting list
			// when stopping to prove that these are discarded.
			time.Sleep(500 * time.Millisecond)
		}
		done <- true
	}()

	pool.waitGroup.Wait()
	<-done

	if debug {
		fmt.Println("Produced: ", atomic.LoadInt32(&produced))
		fmt.Println("Consumed: ", atomic.LoadInt32(&consumed))
		fmt.Println("Waiting Queue : ", pool.WaitingJobs())
		fmt.Println("Finished Queue: ", pool.FinishedJobs())
		fmt.Println("Consumed: ", atomic.LoadInt32(&consumed))
	}
	assert.Less(t, consumed, produced)
	assert.Greater(t, pool.WaitingJobs(), 0)
	assert.EqualValues(t, 0, pool.FinishedJobs())
}

func TestStressWorkerPoolTwo(t *testing.T) {
	t.SkipNow()
	t.Parallel()
	for i := 0; i < 100; i++ {
		t.Run("Stress", TestWorkerPoolTwo)
	}
}

// This uses two separate consumer threads to read results
// and a timer to close the WorkerPool.
// It also uses two producers
func TestWorkerPoolTwo(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU()
	bufferSize := 100
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	done := make(chan bool)
	consumed := int32(0)
	produced := int32(0)

	go func() {
		time.Sleep(5 * time.Second)
		_ = pool.Close()
		if debug {
			fmt.Println("Closing WorkerPool =====================")
		}
	}()

	// producer 1
	go func() {
		for {
			job := &WorkPackage{
				jobID:  int(atomic.LoadInt32(&produced)),
				f:      1000000.0,
				div:    1.000001,
				result: 0,
			}
			err := pool.QueueJob(job)
			if err != nil {
				if debug {
					log.Printf("P1 could not add job %d\n", job.jobID)
				}
				break
			}
			if debug {
				fmt.Printf("P1 add job: %d\n", job.jobID)
			}
			atomic.AddInt32(&produced, 1)
		}
	}()

	// producer 2
	go func() {
		for {
			job := &WorkPackage{
				jobID:  int(atomic.LoadInt32(&produced)),
				f:      1000000.0,
				div:    1.000001,
				result: 0,
			}
			err := pool.QueueJob(job)
			if err != nil {
				if debug {
					log.Printf("P2 could not add job %d\n", job.jobID)
				}
				break
			}
			if debug {
				fmt.Printf("P2 add job: %d\n", job.jobID)
			}
			atomic.AddInt32(&produced, 1)
		}
	}()

	// consumer 1
	go func() {
		for {
			job, closed := pool.GetFinishedWait()
			if closed {
				break
			}
			if job != nil {
				if debug {
					fmt.Printf("C1 Result for %s: %s\n", job.Id(), job.(*WorkPackage).result)
				}
				atomic.AddInt32(&consumed, 1)
			}
		}
		done <- true
	}()

	// consumer 2
	go func() {
		for {
			job, closed := pool.GetFinishedWait()
			if closed {
				break
			}
			if job != nil {
				if debug {
					fmt.Printf("C2 Result for %s: %s\n", job.Id(), job.(*WorkPackage).result)
				}
				atomic.AddInt32(&consumed, 1)
			}
		}
		done <- true
	}()

	// wait for both go consumer routines
	<-done
	<-done

	if debug {
		fmt.Println("Produced: ", atomic.LoadInt32(&produced))
		fmt.Println("Consumed: ", atomic.LoadInt32(&consumed))
		fmt.Println("Waiting Queue : ", pool.WaitingJobs())
		fmt.Println("Finished Queue: ", pool.FinishedJobs())
		fmt.Println("Consumed: ", atomic.LoadInt32(&consumed))
	}
	assert.EqualValues(t, produced, consumed)
	assert.EqualValues(t, 0, pool.WaitingJobs())
}

// Two producers. Finished jobs are ignored.
func TestWorkerPoolProduceOnly(t *testing.T) {
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 1000
	pool := NewWorkerPool(noOfWorkers, bufferSize, false)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	done := make(chan bool)
	produced := int32(0)

	go func() {
		time.Sleep(10 * time.Second)
		_ = pool.Close()
	}()

	// producer 1
	go func() {
		for {
			atomic.AddInt32(&produced, 1)
			job := &WorkPackage{
				jobID:  int(produced),
				f:      1000000.0,
				div:    1.000001,
				result: 0,
			}
			if debug {
				fmt.Printf("P1 adds job: %d\n", produced)
			}
			err := pool.QueueJob(job)
			if err != nil {
				if debug {
					log.Println("could not add job")
				}
				break
			}
		}
		done <- true
	}()

	// producer 2
	go func() {
		for {
			atomic.AddInt32(&produced, 1)
			job := &WorkPackage{
				jobID:  int(produced),
				f:      1000000.0,
				div:    1.000001,
				result: 0,
			}
			if debug {
				fmt.Printf("P2 adds job: %d\n", produced)
			}
			err := pool.QueueJob(job)
			if err != nil {
				if debug {
					log.Println("could not add job")
				}
				break
			}
		}
		done <- true
	}()

	<-done
	<-done

	job, err := pool.GetFinished()
	assert.Nil(t, job)
	assert.NotNil(t, err)
	job, err = pool.GetFinishedWait()
	assert.Nil(t, job)
	assert.NotNil(t, err)

}

type ErrTest struct {
	err error
}

func (et *ErrTest) Id() string {
	return "Test"
}

func (et *ErrTest) Run() error {
	time.Sleep(1 * time.Second)
	et.err = errors.New("test error")
	return et.err
}

// This test checks that an error in the provided job will be logged
// and it is possible to store the err in the job instance and later
// access from the finished job.
// Logging will only be tested visually
func TestErrorInJob(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	// create a special test job which produces an error
	job := &ErrTest{}

	err := pool.QueueJob(job)
	if err != nil {
		log.Println("could not add job")
	}

	_ = pool.Close()
	pool.waitGroup.Wait()

	f, _:= pool.GetFinishedWait()

	// check that the finished job has an err stored.
	assert.EqualValues(t, errors.New("test error"), f.(*ErrTest).err)
}


type PanicTest struct {
}

func (p *PanicTest) Id() string {
	return "Test"
}

func (p *PanicTest) Run() error {
	time.Sleep(1 * time.Second)
	panic("panic")
	return nil
}

// This test checks that a panic in the provided job will be caught
// Logging will only be tested visually
//
func TestPanicInJob(t *testing.T) {
	t.Parallel()
	noOfWorkers := runtime.NumCPU() * 2
	bufferSize := 50
	pool := NewWorkerPool(noOfWorkers, bufferSize, true)
	assert.EqualValues(t, noOfWorkers, pool.workersRunning)

	// create a special test job which produces an error
	job := &PanicTest{}

	err := pool.QueueJob(job)
	if err != nil {
		log.Println("could not add job")
	}

	_ = pool.Close()
	pool.waitGroup.Wait()

	f, _:= pool.GetFinishedWait()

	// check that the finished job has no err stored as the
	// job itself has no panic handling
	assert.NotNil(t, f)
}


// TODO: Benchmark starting a go func directly vs. queueing a job
