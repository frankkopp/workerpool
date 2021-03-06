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

// Package workerpool provides a worker pool implementation using go channels
// The motivation for this package was to learn the Go way of creating a
// worker pool with channels avoiding "traditional" state managed concurrency.
// See the provided README for more detailed information.
// https://github.com/frankkopp/WorkerPool/blob/master/README.md
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const debug = false

// Job is the interface that needs to be satisfied
// for workerpool jobs
type Job interface {
	Run() error
}

// WorkerPool is a set of workers waiting for jobs to be queued
// and executed. Create a new instance with
//  NewWorkerPool()
type WorkerPool struct {
	waitGroup      sync.WaitGroup
	noOfWorkers    int
	workersRunning int32
	startupDone    chan bool

	// number of job in progress
	// This number is inherently volatile. The moment this function
	// returns it is already out of date a concurrently running
	// go routine might have written or read from the channel.
	working int32

	// number of jobs ingested and not yet returned
	// This number is inherently volatile. The moment this function
	// returns it is already out of date a concurrently running
	// go routine might have queued or retrieved from the workerpool.
	jobCount int32

	// flag for storing finished jobs or not
	queueFinished bool

	// define channels for job queues and finished jobs
	bufferSize int
	jobs       chan Job
	finished   chan Job

	// context to close job queue
	ingest context.Context
	close  context.CancelFunc

	// context to stop (stop) workers and release retrievers
	process context.Context
	stop    context.CancelFunc
}

// NewWorkerPool creates a new WorkerPool and immediately
// starts the workers.
// The number of workers created is given by noOfWorkers and
// there can be bufferSize jobs queued before the channel
// blocks the queuing side to wait for free queue space
func NewWorkerPool(noOfWorkers int, bufferSize int, queueFinished bool) *WorkerPool {

	process, stopProcessing := context.WithCancel(context.Background())
	ingest, closeJobQueue := context.WithCancel(context.Background())

	var pool = &WorkerPool{
		waitGroup:      sync.WaitGroup{},
		noOfWorkers:    noOfWorkers,
		workersRunning: 0,
		startupDone:    make(chan bool),
		working:        0,
		jobCount:       0,
		queueFinished:  queueFinished,
		bufferSize:     bufferSize,
		jobs:           make(chan Job, bufferSize),
		finished:       make(chan Job, bufferSize),
		ingest:         ingest,
		close:          closeJobQueue,
		process:        process,
		stop:           stopProcessing,
	} // start workers
	for i := 1; i <= pool.noOfWorkers; i++ {
		go pool.worker(i)
	}

	// wait until all workers are running
	<-pool.startupDone

	return pool
}

// QueueJob adds a new job to the queue of jobs to be execute by the workers
// If the queue is full this blocks until a worker has taken a job from the
// queue
func (pool *WorkerPool) QueueJob(job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint("not accepting new jobs: ", r))
		}
	}()
	select {
	case <-pool.ingest.Done():
		return errors.New(fmt.Sprint("not accepting new jobs as queue has been closed "))
	case pool.jobs <- job:
		atomic.AddInt32(&pool.jobCount, 1)
		return nil
	}
}

// Close tells the WorkerPool to not accept any new jobs and
// to close down after all queued and running jobs are finished.
// This function returns immediately but closing down all
// workers might take a while depending on the size of the
// waiting jobs queue and the duration of jobs.
func (pool *WorkerPool) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint("Jobs queue could not be closed: ", r))
		}
	}()
	// stop ingesting new jobs
	pool.close()
	close(pool.jobs)
	return nil
}

// Stop shuts downs the WorkerPool as soon as possible
// omitting waiting jobs not yet started. This will wait
// (block) until all workers are stopped.
func (pool *WorkerPool) Stop() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint("Jobs queue could not be closed: ", r))
		}
	}()
	// stop ingesting new jobs
	pool.close()
	close(pool.jobs)
	// tell the workers to stop as soon as possible
	// when a worker is running a job this will stop
	// after completion of this job.
	// loop until all workers have stopped
	pool.stop()
	// wait until all workers are terminated
	pool.waitGroup.Wait()
	return nil
}

// GetFinished returns finished jobs if any available.
// It is non blocking and will either return a Job
// or nil. In case of nil it also signals if the
// WorkerPool is done and no more results are to be
// expected.
// If the WorkerPool has been started with
// queueFinished=false then this returns immediately
// with nil and true.
func (pool *WorkerPool) GetFinished() (Job, bool) {
	if !pool.queueFinished {
		return nil, true
	}

	// when all workers are stopped and the finished jobs
	// queue is empty we can return immediately and tell
	// the caller, that this WorkerPool is done.
	if atomic.LoadInt32(&pool.workersRunning) == 0 &&
		pool.FinishedJobs() == 0 {
		return nil, true
	}

	// Try to get a finished job to return.
	// If channel is closed return nil and signal closed.
	// Or return the job (might be nil as well at) but
	// signal "not closed"
	select {
	case job := <-pool.finished:
		atomic.AddInt32(&pool.jobCount, -1)
		// channel not closed return result
		return job, false
	default:
		// no result available - return directly
		return nil, false
	}
}

// GetFinishedWait returns finished jobs if any available
// or blocks and waits until finished jobs are available.
// If the WorkerPool finished queue is closed this returns
// nil and true.
// If the WorkerPool has been started with
// queueFinished=false then this returns immediately
// with nil and true.
func (pool *WorkerPool) GetFinishedWait() (Job, bool) {
	if !pool.queueFinished {
		return nil, true
	}

	// when all workers are stopped and the finished jobs
	// queue is empty we can return immediately and tell
	// the caller, that this WorkerPool is done.
	if atomic.LoadInt32(&pool.workersRunning) == 0 && pool.FinishedJobs() == 0 {
		return nil, true
	}

	// try getting a finished job or wait until one
	// is available or the channel is closed or the
	// WorkerPool has been closed.
	select {
	case job, ok := <-pool.finished:
		if !ok {
			return nil, true
		}
		atomic.AddInt32(&pool.jobCount, -1)
		return job, false
	}
}

// ///////////////////////////////
// Worker
func (pool *WorkerPool) worker(id int) {

	// Startup
	// Register with the wait group and increase worker counter
	// Last worker to start up signals that startup is done
	pool.waitGroup.Add(1)
	if atomic.AddInt32(&pool.workersRunning, 1) == int32(pool.noOfWorkers) {
		pool.startupDone <- true
	}

	// Shutdown
	// make sure to tell the wait group you're done
	// and decrease the workersRunning counter.
	// Unfortunately WaitGroup does not have external
	// access to its counters
	defer func() {
		if atomic.AddInt32(&pool.workersRunning, -1) == 0 {
			// as we closed down all the workers now no more
			// jobs will be finished so we close the channel
			// to release any waiting retrievers.
			close(pool.finished)
		}
		pool.waitGroup.Done()
	}()

	// Running
	for { // loop for querying incoming jobs or stop signal
		select {
		case <-pool.process.Done():
			return
		case job, ok := <-pool.jobs: // check if a job is available
			// channel close and empty
			if !ok {
				return
			}
			// ignore nil jobs when the channel is not closed
			if job == nil {
				continue
			}

			atomic.AddInt32(&pool.working, 1)

			// Run the job by calling its Run() function
			// Real error handling needs to be done in the
			// job's Run() itself and stored into the Job
			// instance.
			pool.runIt(job)

			// storing the job in the finished queue
			// if the finished channel is full this waits here until
			// it either is emptied by another thread or the stop signal
			// is received
			if pool.queueFinished {
				pool.finished <- job
			}

			atomic.AddInt32(&pool.working, -1)
		} // select
	} // for
}

// we call the job's Run() in a separate function to be able to catch
// any panics the job might throw.
func (pool *WorkerPool) runIt(job Job) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Panic in job %s: %v\n", job, err)
		}
	}()
	// Run the job by calling its Run() function
	// Real error handling needs to be done in the
	// job's Run() itself and stored into the Job
	// instance.
	err := job.Run()
	if err != nil {
		log.Printf("Error in job %v: %s\n", job, err)
	}
}

// WaitingJobs returns the number of not yet started jobs
// This number is inherently volatile. The moment this function
// returns it is already out of date as a concurrently running
// go routine might have written or read from the channel.
func (pool *WorkerPool) WaitingJobs() int {
	return len(pool.jobs)
}

// FinishedJobs returns the number of finished jobs
// This number is inherently volatile. The moment this function
// returns it is already out of date a concurrently running
// go routine might have written or read from the channel.
func (pool *WorkerPool) FinishedJobs() int {
	return len(pool.finished)
}

// RunningJobs returns the number of jobs currently running
// This number is inherently volatile. The moment this function
// returns it is already out of date a concurrently running
// go routine might have written or read from the channel.
func (pool *WorkerPool) RunningJobs() int {
	return int(atomic.LoadInt32(&pool.working))
}

// HasJobs returns true if there is at least one job still in to
// to process or retrieve
// This is inherently volatile. The moment this function
// returns it is already out of date a concurrently running
// go routine might have queued or retrieved from the workerpool.
func (pool *WorkerPool) HasJobs() bool {
	return int(atomic.LoadInt32(&pool.jobCount)) > 0
}

// Jobs returns the total number of Jobs in the WorkerPool
// This number is inherently volatile. The moment this function
// returns it is already out of date a concurrently running
// go routine might have queued or retrieved from the workerpool.
func (pool *WorkerPool) Jobs() int {
	return int(atomic.LoadInt32(&pool.jobCount))
}

// Active returns true if the WorkPool workers are still active
func (pool *WorkerPool) Active() bool {
	return atomic.LoadInt32(&pool.workersRunning) > 0
}
