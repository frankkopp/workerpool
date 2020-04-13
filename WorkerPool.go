package WorkerPool

import (
	"errors"
	"fmt"
	"log"
	"runtime"
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

	// number of job in progress - TODO not transactional yet
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

	if debug {
		fmt.Printf("Starting %d workers\n", pool.noOfWorkers)
	}

	for i := 1; i <= pool.noOfWorkers; i++ {
		pool.waitGroup.Add(1)
		pool.workersRunning++
		go worker(pool, i)
	}

	return pool
}

// QueueJob adds a new job to the queue of jobs to be execute by the workers
// If the queue is full this blocks until a worker has taken a job from the
// queue
func (wp *WorkerPool) QueueJob(r Job) (err error) {
	if wp.closedFlag {
		err = errors.New("not accepting new jobs as the WorkerPool queue is closed")
		return err
	}
	// add the job to the queue or wait until the queue has space
	wp.jobs <- r
	return nil
}

// Close tells the WorkerPool to not accept any new jobs and
// to close down after all queued and running jobs are finished.
// This function returns immediately but closing down all
// workers might take a while depending on the size of the
// waiting jobs queue and the duration of jobs.
func (wp *WorkerPool) Close() {
	// already closed
	if wp.closedFlag {
		return
	}

	if debug {
		fmt.Printf("Closing WorkerPool. Finishing %d jobs\n", len(wp.jobs))
	}

	// Set the closed flag so no new jobs are accepted.
	// Then close the channel
	wp.closedFlag = true

	// Tell the workers to check for closed signal as
	// soon as possible.
	// Loop until all workers have closed down
	// TODO Avoid this busy loop
	go func() {
		for {
			if wp.workersRunning > 0 {
				select {
				case wp.closed <- true:
				default:
					runtime.Gosched()
				}
			} else {
				close(wp.jobs)
				break
			}
		}
	}()
}

// Stop shuts downs the WorkerPool as soon as possible
// omitting waiting jobs not yet started. This will wait
// (block) until all workers are stopped.
func (wp *WorkerPool) Stop() {
	// to gracefully protect against the panic
	// when the jobs channel is closed twice
	defer func() {
		recover()
	}()

	if debug {
		fmt.Printf("Stopping WorkerPool. Skipping %d jobs\n", len(wp.jobs))
	}

	// Set the closed flag so no new jobs are accepted.
	wp.closedFlag = true

	// tell the workers to stop as soon as possible
	// when a worker is running a job this will stop
	// after completion of this job.
	// loop until all workers have stopped
	for {
		if wp.workersRunning > 0 {
			select {
			case wp.stop <- true:
			default:
				runtime.Gosched()
			}
		} else {
			close(wp.jobs)
			break
		}
	}
}

// Shutdown stops the WorkerPool and closes the finished channel.
// No more queries to the finished queue will be accepted.
// Also releases already waiting queries to the finished channel.
func (wp *WorkerPool) Shutdown() {
	wp.Stop()
	close(wp.finished)
}

// GetFinished returns finished jobs if any available.
// It is non blocking and will either return a Job
// or nil. In case of nil it also signals if the
// WorkerPool is done and no more results are to be
// expected.
// If the WorkerPool has been started with
// queueFinished=false then this returns immediately
// with nil and true.
func (wp *WorkerPool) GetFinished() (Job, bool) {
	if !wp.queueFinished {
		return nil, true
	}
	// Try to get a finished job to return.
	// If channel is closed return nil and signal closed
	// of return the job (might be nil as well at) but
	// signal "not closed"
	select {
	case job, ok := <-wp.finished:
		if !ok { // channel closed
			return nil, true
		} else { // channel not closed return result
			if debug {
				fmt.Println("succeeded to get result", job.Id())
			}
			return job, false
		}
	default:
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
func (wp *WorkerPool) GetFinishedWait() (Job, bool) {
	if !wp.queueFinished {
		return nil, true
	}
	// try to get a finished job to return.
	// if no finished job available check if any
	// workers are still running.
	// TODO improve this without busy wait
	for {
		select {
		case job, ok := <-wp.finished:
			if !ok {
				if wp.workersRunning == 0 && len(wp.finished) == 0 {
					return nil, true
				}
			} else {
				if debug {
					fmt.Println("succeeded to get result", job.Id())
				}
				return job, false
			}
		default:
			runtime.Gosched()
		}
	}
}

// WaitingJobs returns the number of not yet started jobs
func (wp *WorkerPool) WaitingJobs() int {
	return len(wp.jobs)
}

// FinishedJobs returns the number of finished jobs
func (wp *WorkerPool) FinishedJobs() int {
	return len(wp.finished)
}

// RunningJobs returns the number of jobs currently running
func (wp *WorkerPool) RunningJobs() int {
	// TODO: this is not transactional - there is a gap between
	//  Waiting Jobs and this number as I do not know a good way
	//  to synchronize <- channel reads
	return wp.working
}

// HasJobs returns true if there is at least one job still in to
// to process or retrieve
func (wp *WorkerPool) HasJobs() bool {
	return wp.Jobs() > 0
}

// Jobs returns the total number of Jobs in the WorkerPool
func (wp *WorkerPool) Jobs() int {
	// TODO: this is not transactional - there is a gap between
	//  Waiting Jobs and working as I do not know a good way
	//  to synchronize <- channel reads
	return wp.working + len(wp.finished) + len(wp.jobs)
}

// Active return true if the WorkPool workers are still active
func (wp *WorkerPool) Active() bool {
	return wp.workersRunning > 0
}

// ///////////////////////////////
// Worker
func worker(wp *WorkerPool, id int) {
	// make sure to tell the wait group you're done
	// and decrease the workersRunning counter.
	// Unfortunately WaitGroup does not has external
	// access to its counters
	defer func() {
		wp.workersRunning--
		wp.waitGroup.Done()
		if debug {
			fmt.Printf("Worker %d is shutting down\n", id)
		}
	}()

	if debug {
		fmt.Printf("Worker %d started\n", id)
	}

	for { // loop for querying incoming jobs or stop signal
		select {
		case job := <-wp.jobs: // check if a job is available
			// ignore nil jobs
			if job == nil {
				continue
			}
			// TODO: this is not transactional - there is a gap between
			//  Waiting Jobs and working as I do not know a good way
			//  to synchronize <- channel reads
			wp.working++

			if debug {
				fmt.Printf("Worker %d started job: %s\n", id, job.Id())
			}

			// Run the job by calling its Run() function
			// Real error handling needs to be done in the
			// job's Run() itself and stored into the Job
			// instance.
			// IDEA: Use another channel/queue for failed job's?
			if err := job.Run(); err != nil {
				if debug {
					log.Printf("Error in job %s: %s\n", job.Id(), err)
				}
			}

			// storing the job in the finished queue
			// if the finished channel is full this waits here until
			// it either is emptied by another thread or the stop signal
			// is received
			if wp.queueFinished {
				select {
				case wp.finished <- job:
					wp.working--
				case <-wp.stop:
					return
				}
			}
		case <-wp.closed: // check for a closed signal
			// if there are still waiting jobs re-signal
			// the closed signal and loop again.
			if len(wp.jobs) == 0 {
				return
			} else {
				wp.closed <- true
			}
		case <-wp.stop: // check for a stop signal
			return
		} // select
	} // for
}
