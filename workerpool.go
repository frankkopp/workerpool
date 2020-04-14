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

// Package workerpool provides a worker pool implementation using channels
// internally without exposing them to the external user.
// Usage:
//  See https://github.com/frankkopp/WorkerPool/blob/master/README.md
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
	Id() string
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
	// TODO: this is not transactional - there is a gap between
	//  Waiting Jobs and this number as I do not know a good way
	//  to synchronize <-channel reads yet
	working int

	// flag for storing finished jobs or not
	queueFinished bool

	// define channels for job queues and finished jobs
	bufferSize int
	jobs       chan Job
	finished   chan Job

	// context to cancel (stop) workers and release retrievers
	ctx    context.Context
	cancel context.CancelFunc
}

// NewWorkerPool creates a new WorkerPool and immediately
// starts the workers.
// The number of workers created is given by noOfWorkers and
// there can be bufferSize jobs queued before the channel
// blocks the queuing side to wait for free queue space
func NewWorkerPool(noOfWorkers int, bufferSize int, queueFinished bool) *WorkerPool {

	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		waitGroup:      sync.WaitGroup{},
		noOfWorkers:    noOfWorkers,
		workersRunning: 0,
		startupDone:    make(chan bool),
		working:        0,
		queueFinished:  queueFinished,
		bufferSize:     bufferSize,
		jobs:           make(chan Job, bufferSize),
		finished:       make(chan Job, bufferSize),
		ctx:            ctx,
		cancel:         cancel,
	}

	// start workers
	for i := 1; i <= pool.noOfWorkers; i++ {
		go pool.worker(ctx, i)
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
	pool.jobs <- job
	return nil
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
	// tell the workers to stop as soon as possible
	// when a worker is running a job this will stop
	// after completion of this job.
	// loop until all workers have stopped
	pool.cancel()
	// close the input queue to not accept any more jobs
	close(pool.jobs)
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
	if atomic.LoadInt32(&pool.workersRunning) == 0 && pool.FinishedJobs() == 0 {
		return nil, true
	}

	// Try to get a finished job to return.
	// If channel is closed return nil and signal closed.
	// Or return the job (might be nil as well at) but
	// signal "not closed"
	select {
	case <-pool.ctx.Done():
		return nil, true
	case job, ok := <-pool.finished:
		if !ok { // channel closed
			return nil, true
		}
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
		return job, false
	}
}

// ///////////////////////////////
// Worker
func (pool *WorkerPool) worker(ctx context.Context, id int) {

	// Startup
	pool.waitGroup.Add(1)
	if atomic.AddInt32(&pool.workersRunning, 1) == int32(pool.noOfWorkers) {
		pool.startupDone <- true
	}
	if debug {
		fmt.Printf("Worker %d starting.\n", id)
	}

	// Shutdown
	// make sure to tell the wait group you're done
	// and decrease the workersRunning counter.
	// Unfortunately WaitGroup does not has external
	// access to its counters
	defer func() {
		if atomic.AddInt32(&pool.workersRunning, -1) == 0 {
			// as we closed down all the workers now no more
			// jobs will be finished so we close the channel
			// to release any waiting retrievers.
			close(pool.finished)
		}
		if debug {
			fmt.Printf("Worker %d shutting down\n", id)
		}
		pool.waitGroup.Done()
	}()

	// Running
	for { // loop for querying incoming jobs or stop signal
		select {
		case <-ctx.Done():
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

			// TODO: this is not transactional - there is a gap between
			//  Waiting Jobs and working as I do not know a good way
			//  to synchronize <- channel reads
			pool.working++

			// fmt.Printf("Worker %d started job: %s\n", id, job.Id())

			// Run the job by calling its Run() function
			// Real error handling needs to be done in the
			// job's Run() itself and stored into the Job
			// instance.
			// IDEA: Use another channel/queue for failed job's?
			err := job.Run()
			if err != nil {
				log.Printf("Error in job %s: %s\n", job.Id(), err)
			}

			// storing the job in the finished queue
			// if the finished channel is full this waits here until
			// it either is emptied by another thread or the stop signal
			// is received
			if pool.queueFinished {
				select {
				case <-ctx.Done():
					return
				case pool.finished <- job:
					pool.working--
				}
			}
		} // select
	} // for
}

// WaitingJobs returns the number of not yet started jobs
func (pool *WorkerPool) WaitingJobs() int {
	return len(pool.jobs)
}

// FinishedJobs returns the number of finished jobs
func (pool *WorkerPool) FinishedJobs() int {
	return len(pool.finished)
}

// RunningJobs returns the number of jobs currently running
func (pool *WorkerPool) RunningJobs() int {
	// TODO: this is not transactional - there is a gap between
	//  Waiting Jobs and this number as I do not know a good way
	//  to synchronize <- channel reads
	return pool.working
}

// HasJobs returns true if there is at least one job still in to
// to process or retrieve
func (pool *WorkerPool) HasJobs() bool {
	return pool.Jobs() > 0
}

// Jobs returns the total number of Jobs in the WorkerPool
// Obs:
//  This is not transactional - there is a gap between
//	waiting jobs and working jobs as I do not know a good way
//	to synchronize <-channel reads yet
func (pool *WorkerPool) Jobs() int {
	// TODO: this is not transactional - there is a gap between
	//  Waiting Jobs and working as I do not know a good way
	//  to synchronize <- channel reads
	return pool.working + len(pool.finished) + len(pool.jobs)
}

// Active return true if the WorkPool workers are still active
func (pool *WorkerPool) Active() bool {
	return pool.workersRunning > 0
}
