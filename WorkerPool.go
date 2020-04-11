package WorkerPool

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

// Runnable
// This interface needs to be satisfied for the WorkerPool Queue
type Runnable interface {
	Run() error
	Id() string
}

// WorkerPool is a set of workers waiting for jobs to be queued and executed.
// Create with
//  NewWorkerPool()
type WorkerPool struct {
	waitGroup      sync.WaitGroup
	noOfWorkers    int
	workersRunning int
	jobs           chan Runnable
	bufferSize     int

	// these control the state of the worker pool
	stateMutex sync.Mutex
	closed     bool
	stopped    bool

	// these control the jobs
	jobMutex   sync.Mutex
	finished   []Runnable
	jobsQueued int
}

// NewWorkerPool creates a new pool of Workers and immediately
// starts them.
// The number of workers created is given by noOfWorkers and
// the there can be bufferSize jobs queued before the channel
// blocks the queuing side to wait for free queue space
func NewWorkerPool(noOfWorkers int, bufferSize int) *WorkerPool {

	wp := &WorkerPool{
		waitGroup:      sync.WaitGroup{},
		noOfWorkers:    noOfWorkers,
		bufferSize:     bufferSize,
		jobs:           make(chan Runnable, bufferSize),
		stateMutex:     sync.Mutex{},
		closed:         false,
		stopped:        false,
		jobMutex:       sync.Mutex{},
		finished:       make([]Runnable, 0, bufferSize),
		jobsQueued:     0,
		workersRunning: 0,
	}

	fmt.Printf("Starting %d workers\n", wp.noOfWorkers)
	for w := 1; w <= wp.noOfWorkers; w++ {
		wp.jobMutex.Lock()
		wp.waitGroup.Add(1)
		wp.workersRunning++
		go worker(wp, w)
		wp.jobMutex.Unlock()
	}

	return wp
}

// QueueJob adds a new job to the queue of jobs to be execute by the workers
func (wp *WorkerPool) QueueJob(r Runnable) error {
	wp.stateMutex.Lock()
	defer wp.stateMutex.Unlock()
	// if the worker pool is not closed or stopped
	// add new job
	if !wp.closed && !wp.stopped {
		wp.jobs <- r
		wp.jobMutex.Lock()
		wp.jobsQueued++
		wp.jobMutex.Unlock()
		return nil
	}
	return errors.New("not accepting new jobs as WorkerPool is closed or stopped")
}

// Close tells the worker pool to shut down after all outstanding
// jobs are finished
func (wp *WorkerPool) Close() {
	fmt.Printf("Closing WorkerPool. Finishing %d jobs\n", len(wp.jobs))
	// Set the closed flag so no new jobs are accepted.
	// Then close the channel
	wp.stateMutex.Lock()
	wp.closed = true
	close(wp.jobs)
	wp.stateMutex.Unlock()
}

// Stop shuts downs the WorkerPool as soon as possible
// omitting open jobs.
func (wp *WorkerPool) Stop() {
	fmt.Printf("Stopping WorkerPool. Skipping %d jobs\n", len(wp.jobs))
	// add nil jobs to the queue to wake up waiting workers
	// so they can see the stopped flag.
	// Unfortunately I do not yet know how to interrupt a
	// channel read operation.
	for i := 0; i < wp.noOfWorkers; i++ {
		wp.jobs <- nil
	}

	// close the channel and signal the workers to stop
	// immediately
	wp.stateMutex.Lock()
	wp.closed = true
	close(wp.jobs)
	wp.stopped = true
	wp.stateMutex.Unlock()
}

// GetFinished returns finished jobs
func (wp *WorkerPool) GetFinished() (Runnable, bool) {
	wp.jobMutex.Lock()
	defer wp.jobMutex.Unlock()

	// pool is closed, all jobs are taken and no workers
	// running, no more results
	if wp.workersRunning == 0 && len(wp.finished) == 0 {
		return nil, true
	}

	// workers are still running and we have finished jobs
	if len(wp.finished) > 0 {
		f := wp.finished[0]

		wp.finished = wp.finished[1:]
		// if finished list is empty take the chance to release
		// the underlying array for the garbage collector
		if len(wp.finished) == 0 {
			wp.finished = make([]Runnable, 0, wp.bufferSize)
		}
		return f, false
	}
	// workers are still running  but we don't have a finished job
	// return nil but not done to signal that we are not done
	return nil, false
}

// ///////////////////////////////
// Worker
func worker(wp *WorkerPool, id int) {
	// make sure to reduce the wait group and running
	// worker counters.
	// Unfortunately WaitGroup does not has external
	// access to its counters
	defer func() {
		wp.jobMutex.Lock()
		wp.workersRunning--
		wp.waitGroup.Done()
		wp.jobMutex.Unlock()
	}()

	fmt.Printf("Worker %d started\n", id)
	for j := range wp.jobs {

		// if we get a nil job it usually signals that
		// we should stop. if not just continue and take
		// the next job.
		if j == nil {
			if wp.stopped {
				return
			}
			continue
		}

		fmt.Printf("Worker %d started job: %s\n", id, j.Id())

		// run the job by calling its Run() function
		if err := j.Run(); err != nil {
			log.Printf("Error in job %s: %s\n", j.Id(), err)
		}

		// store the kob in the finished jobs array
		wp.jobMutex.Lock()
		wp.finished = append(wp.finished, j)
		wp.jobMutex.Unlock()

		// if the pool has been stopped close the the worker down and return
		if wp.stopped {
			return
		}
	}
	fmt.Printf("Worker %d is shutting down\n", id)
}
