package WorkerPool

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

const debug = false

// Job
// This interface needs to be satisfied for the WorkerPool Queue
type Job interface {
	Run() error
	Id() string
}

// WorkerPool is a set of workers waiting for jobs to be queued
// and executed. Create anew instance with
//  NewWorkerPool()
type WorkerPool struct {
	waitGroup      sync.WaitGroup
	noOfWorkers    int
	workersRunning int

	// number of job in progress
	// TODO: this is not transactional - there is a gap between
	//  Waiting Jobs and this number as I do not know a good way
	//  to synchronize <- channel reads
	working int

	// flag for storing finished jobs or not
	queueFinished bool

	// define channels for job queues and finished jobs
	bufferSize int
	jobs       chan Job
	finished   chan Job
	stop       chan bool
	closed     chan bool
	closedFlag bool
}

// NewWorkerPool creates a new WorkerPool and immediately
// starts the workers.
// The number of workers created is given by noOfWorkers and
// there can be bufferSize jobs queued before the channel
// blocks the queuing side to wait for free queue space
func NewWorkerPool(noOfWorkers int, bufferSize int, queueFinished bool) *WorkerPool {

	pool := &WorkerPool{
		waitGroup:      sync.WaitGroup{},
		noOfWorkers:    noOfWorkers,
		workersRunning: 0,
		working:        0,
		queueFinished:  queueFinished,
		bufferSize:     bufferSize,
		jobs:           make(chan Job, bufferSize),
		finished:       make(chan Job, bufferSize),
		stop:           make(chan bool),
		closed:         make(chan bool),
		closedFlag:     false,
	}

	for i := 1; i <= pool.noOfWorkers; i++ {
		pool.waitGroup.Add(1)
		pool.workersRunning++
		go pool.worker(i)
	}

	for pool.workersRunning < pool.noOfWorkers {
	}

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
	pool.stop <- true
	// close the input queue to not accept any more jobs
	close(pool.jobs)
	return nil
}

// Shutdown stops the WorkerPool and closes the finished channel.
// No more queries to the finished queue will be accepted.
// Also releases already waiting queries to the finished channel.
func (pool *WorkerPool) Shutdown() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint("Finished queue could not be closed: ", r))
		}
	}()
	e := pool.Stop()
	close(pool.finished)
	return e
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
	// Try to get a finished job to return.
	// If channel is closed return nil and signal closed.
	// Or return the job (might be nil as well at) but
	// signal "not closed"
	select {
	case _, ok := <-pool.stop:
		if !ok {
			return nil, true
		}
		pool.stop <- true
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
	// try getting a finished job or wait until one
	// is available or the channel is closed
	select {
	case _, ok := <-pool.stop:
		if !ok {
			return nil, true
		}
		pool.stop <- true
		return nil, true
	case job, ok := <-pool.finished:
		if !ok {
			return nil, true
		}
		return job, false
	}
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

// ///////////////////////////////
// Worker
func (pool *WorkerPool) worker(id int) {
	// make sure to tell the wait group you're done
	// and decrease the workersRunning counter.
	// Unfortunately WaitGroup does not has external
	// access to its counters
	defer func() {
		pool.workersRunning--
		pool.waitGroup.Done()
		// last one turns the light out
		if pool.workersRunning == 0 {
			close(pool.finished)
		}
		fmt.Printf("Worker %d is shutting down\n", id)
	}()

	if debug {
		fmt.Printf("Worker %d started\n", id)
	}

	for { // loop for querying incoming jobs or stop signal
		select {
		case <-pool.stop: // check for a stop signal
			pool.stop <- true
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
				case pool.finished <- job:
					pool.working--
				case <-pool.stop:
					pool.stop <- true
					return
				}
			}
		} // select
	} // for
}
