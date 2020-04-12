package WorkerPool

import (
	"errors"
	"fmt"
	"log"
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
	working        int
	workingMutex   sync.Mutex

	// define channels for job queues and finished jobs
	bufferSize int
	jobs       chan Job
	finished   chan Job
	stop       chan bool
	closed     chan bool
	closedFlag bool

	jobsQueued int
}

// NewWorkerPool creates a new pool of Workers and immediately
// starts them.
// The number of workers created is given by noOfWorkers and
// the there can be bufferSize jobs queued before the channel
// blocks the queuing side to wait for free queue space
func NewWorkerPool(noOfWorkers int, bufferSize int) *WorkerPool {

	pool := &WorkerPool{
		waitGroup:      sync.WaitGroup{},
		noOfWorkers:    noOfWorkers,
		workersRunning: 0,
		working:        0,
		workingMutex:   sync.Mutex{},
		bufferSize:     bufferSize,
		jobs:           make(chan Job, bufferSize),
		finished:       make(chan Job, bufferSize),
		stop:           make(chan bool),
		closed:         make(chan bool),
		closedFlag:     false,
		jobsQueued:     0,
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
// If the queue is full this blocks until a worker has taken a job from the
// queue
func (wp *WorkerPool) QueueJob(r Job) (err error) {
	// to gracefully protect against the panic
	// when the jobs channel is closed
	defer func() {
		if recover() != nil {
			err = errors.New("not accepting new jobs as the WorkerPool queue is closed")
		}
	}()

	if wp.closedFlag {
		err = errors.New("not accepting new jobs as the WorkerPool queue is closed")
		return
	}

	// add the job to the queue or wait until the queue has space
	wp.jobs <- r
	return nil
}

// Close tells the worker pool to not accept any new jobs and
// to shut down after all queued and running jobs are finished
func (wp *WorkerPool) Close() {
	// already closed
	if wp.closedFlag {
		return
	}

	fmt.Printf("Closing WorkerPool. Finishing %d jobs\n", len(wp.jobs))
	// Set the closed flag so no new jobs are accepted.
	// Then close the channel
	wp.closedFlag = true
	// tell the workers to check for closed as soon as possible
	for i := 0; i < wp.noOfWorkers; i++ {
		wp.closed <- true
	}
}

// Stop shuts downs the WorkerPool as soon as possible
// omitting queued jobs not yet started. This will wait
// (block) until all workers are stopped
func (wp *WorkerPool) Stop() {
	// to gracefully protect against the panic
	// when the jobs channel is closed twice
	defer func() {
		recover()
	}()

	fmt.Printf("Stopping WorkerPool. Skipping %d jobs\n", len(wp.jobs))
	// Set the closed flag so no new jobs are accepted.
	wp.closedFlag = true

	// tell the workers to stop as soon as possible
	// when a worker is running a job this will stop
	// after completion of this job
	for {
		if wp.workersRunning > 0 {
			select {
			case wp.stop <- true:
			default:
			}
		} else {
			close(wp.jobs)
			break
		}
	}

	// wait until all workers are shut down.
	wp.waitGroup.Wait()
}

// Shutdown stops the WorkerPool and closes the finished channel.
// Non more queries to the finished queue will be accepted.
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
func (wp *WorkerPool) GetFinished() (Job, bool) {
	// try to get a finished job to return.
	// if no finished job available check if any
	// workers are still running.
	select {
	case job, ok := <-wp.finished:
		if ok {
			fmt.Println("succeeded to get result", job.Id())
			return job, false
		}
		return nil, true
	default:
		if wp.workersRunning == 0 {
			return nil, true
		}
	}
	return nil, false
}

// GetFinishedWait returns finished jobs if any available
// or blocks and waits until finished jobs are available.
// If the WorkerPool finished queue is closed this returns
// nil and true.
func (wp *WorkerPool) GetFinishedWait() (Job, bool) {
	// try to get a finished job to return.
	// if no finished job available check if any
	// workers are still running.
	for {
		select {
		case job, ok := <-wp.finished:
			if ok {
				fmt.Println("succeeded to get result", job.Id())
				return job, false
			}
			return nil, true
		}
	}
}

// OpenJobs returns the number of not yet started jobs
func (wp *WorkerPool) OpenJobs() int {
	return len(wp.jobs)
}

// FinishedJobs returns the number of finished jobs
func (wp *WorkerPool) FinishedJobs() int {
	return len(wp.finished)
}

// InProgress returns the number of jobs currently running
func (wp *WorkerPool) InProgress() int {
	return wp.working
}

// HasJobs returns true if there is at least one job still in to
// to process or retrieve
func (wp *WorkerPool) HasJobs() bool {
	return wp.working+len(wp.finished)+len(wp.jobs) > 0
}

// Jobs returns the total number of Jobs in the WorkerPool
func (wp *WorkerPool) Jobs() int {
	return wp.working + len(wp.finished) + len(wp.jobs)
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
		fmt.Printf("Worker %d is shutting down\n", id)
	}()

	fmt.Printf("Worker %d started\n", id)

	for {

		// loop for querying incoming jobs or stop signal
		select {

		case job := <-wp.jobs:

			if job == nil { // ignore nil jobs
				continue
			}

			wp.working++
			fmt.Printf("Worker %d started job: %s\n", id, job.Id())

			// run the job by calling its Run() function
			if err := job.Run(); err != nil {
				log.Printf("Error in job %s: %s\n", job.Id(), err)
			}

			// storing the job in the finished channel
			// if the finished channel is full this waits here until
			// it either is emptied by another thread or the stop signal
			// is received
			select {
			case wp.finished <- job:
				wp.working--
			case <-wp.stop:
				return
			}

		case <-wp.closed:
			if len(wp.jobs) == 0 {
				return
			} else {
				wp.closed <- true
			}

		case <-wp.stop:
			return

		}

	}
}
