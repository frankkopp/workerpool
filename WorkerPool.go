package WorkerPool

import (
	"errors"
	"fmt"
	"sync"
)

// Job
// This interface needs to be satisfied for the WorkerPool Queue
type Job interface {
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

	// define channels for job queues and finished jobs
	bufferSize int
	jobs       chan Job
	finished   chan Job

	// these control the state of the worker pool
	closed  bool
	stopped bool

	// these control the jobs
	jobsQueued int
}

// NewWorkerPool creates a new pool of Workers and immediately
// starts them.
// The number of workers created is given by noOfWorkers and
// the there can be bufferSize jobs queued before the channel
// blocks the queuing side to wait for free queue space
func NewWorkerPool(noOfWorkers int, bufferSize int) *WorkerPool {

	pool := &WorkerPool{
		waitGroup:   sync.WaitGroup{},
		noOfWorkers: noOfWorkers,

		bufferSize: bufferSize,
		jobs:       make(chan Job, bufferSize),
		finished:   make(chan Job, bufferSize),

		closed:  false,
		stopped: false,

		jobsQueued:     0,
		workersRunning: 0,
	}

	fmt.Printf("Starting %d workers\n", pool.noOfWorkers)
	for i := 1; i <= pool.noOfWorkers; i++ {
		pool.waitGroup.Add(1)
		pool.workersRunning++
		go worker(pool, i)
	}

	return pool
}

// QueueJob adds a new job to the queue of jobs to be execute by the workers
func (wp *WorkerPool) QueueJob(r Job) error {
	// if the worker pool is not closed or stopped
	// add new job
	if !wp.closed && !wp.stopped {
		// 		wp.jobs <- r
		// 		wp.jobsQueued++
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
	wp.closed = true
	close(wp.jobs)
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
	close(wp.jobs)
	wp.closed = true
	wp.stopped = true
	wp.waitGroup.Wait()
	close(wp.finished)

}

// GetFinished returns finished jobs if any available.
// It is non blocking and will either return a Job
// or nil. In case of nil it also signals if the
// WorkerPool is done and no more results are to be
// expected.
func (wp *WorkerPool) GetFinished() (Job, bool) {
	// try to get a finished job to return.
	// if no finished job available check if any
	// workers are still running.
	select {
	case job := <-wp.finished:
		fmt.Println("succeeded to get result", job.Id())
		return job, false
	default:
		fmt.Println("failed to get job")
		if wp.workersRunning == 0 {
			return nil, true
		}
	}
	// workers are still running but we don't have a finished job
	// return nil but "not done" to signal that we are not done
	return nil, false
}

// GetFinishedWait returns finished jobs if any available
// or blocks and waits until finished jobs are available.
// If the WorkerPool is closed this returns nil and true.
func (wp *WorkerPool) GetFinishedWait() (Job, bool) {
	job, open := <-wp.finished
	if !open {
		return nil, true
	}
	fmt.Println("succeeded to get result", job.Id())
	return job, false
}

// ///////////////////////////////
// Worker
func worker(wp *WorkerPool, id int) {
	// make sure to reduce the wait group and running
	// worker counters.
	// Unfortunately WaitGroup does not has external
	// access to its counters
	defer func() {
		wp.workersRunning--
		wp.waitGroup.Done()
	}()

	fmt.Printf("Worker %d started\n", id)

	for j := range wp.jobs {

		// if we get a nil job it usually signals that
		// we should stop. if not just continue and take
		// the next job.
		if j == nil {
			if wp.stopped {
				break
			}
			continue
		}

		fmt.Printf("Worker %d started job: %s\n", id, j.Id())

		// 	// run the job by calling its Run() function
		// 	if err := j.Run(); err != nil {
		// 		log.Printf("Error in job %s: %s\n", j.Id(), err)
		// 	}

		// 	// store the kob in the finished jobs array
		// 	wp.jobMutex.Lock()
		// 	wp.finished = append(wp.finished, j)
		// 	wp.jobMutex.Unlock()

		// 	// if the pool has been stopped close the the worker down and return
		// 	if wp.stopped {
		// 		return
		// 	}
	}

	fmt.Printf("Worker %d is shutting down\n", id)
}
