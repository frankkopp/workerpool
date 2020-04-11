package WorkerPool

import (
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

func runIt(r Runnable) error {
	return r.Run()
}

func getId(r Runnable) string {
	return r.Id()
}

// WorkerPool is a set of workers waiting for jobs to be queued and executed.
// Create with
//  NewWorkerPool()
type WorkerPool struct {
	waitGroup   sync.WaitGroup
	noOfWorkers int
	bufferSize  int
	jobs        chan Runnable

	stop chan bool

	stateMutex sync.Mutex
	closed     bool

	finished       []Runnable
	finishedMutex  sync.Mutex
	workersRunning int32
}

// NewWorkerPool creates a new pool of Workers
// The number of workers created is given by noOfWorkers and
// the there can be bufferSize jobs queued before the channel
// blocks the queuing side to wait for free queue space
func NewWorkerPool(noOfWorkers int, bufferSize int) *WorkerPool {

	wp := &WorkerPool{
		waitGroup:      sync.WaitGroup{},
		noOfWorkers:    noOfWorkers,
		bufferSize:     bufferSize,
		jobs:           make(chan Runnable, bufferSize),
		stop:           make(chan bool),
		stateMutex:     sync.Mutex{},
		closed:         false,
		finished:       make([]Runnable, 0, bufferSize),
		finishedMutex:  sync.Mutex{},
		workersRunning: 0,
	}

	fmt.Printf("Starting %d workers\n", wp.noOfWorkers)
	for w := 1; w <= wp.noOfWorkers; w++ {
		wp.finishedMutex.Lock()
		wp.waitGroup.Add(1)
		wp.workersRunning++
		go worker(wp, w)
		wp.finishedMutex.Unlock()
	}

	return wp
}

// QueueJob adds a new job to the queue of jobs to be execute by the workers
func (wp *WorkerPool) QueueJob(r Runnable) {
	wp.stateMutex.Lock()
	defer wp.stateMutex.Unlock()
	if !wp.closed {
		wp.jobs <- r
	} else {
		log.Println("Could not queue job as WorkerPool was closed")
	}
}

// Close tells the worker pool to shut down after all outstanding
// jobs are finished
func (wp *WorkerPool) Close() {
	fmt.Printf("Closing WorkerPool. Finishing %d jobs\n", len(wp.jobs))
	wp.stateMutex.Lock()
	wp.closed = true
	wp.stateMutex.Unlock()
	close(wp.jobs)
}

// Stop shuts downs the WorkerPool as soon as possible
// omitting open jobs.
func (wp *WorkerPool) Stop() {
	panic("not implemented yet")
}

// GetFinished returns finished jobs
func (wp *WorkerPool) GetFinished() (Runnable, bool) {
	wp.stateMutex.Lock()
	defer wp.stateMutex.Unlock()

	wp.finishedMutex.Lock()
	defer wp.finishedMutex.Unlock()

	// pool is closed, all jobs are taken and no workers running, no more results
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
	// workers are still running and but we don't have a finished job
	// return nil but signal that we are not done
	return nil, false
}

// ///////////////////////////////
// Worker
func worker(wp *WorkerPool, id int) {
	defer func() {
		wp.finishedMutex.Lock()
		wp.workersRunning--
		wp.waitGroup.Done()
		wp.finishedMutex.Unlock()
	}()
	fmt.Printf("Worker %d started\n", id)
	for j := range wp.jobs {
		// fmt.Printf("Job %s started\n", j.Id())
		if err := j.Run(); err != nil {
			log.Printf("Error in job %s: %s\n", j.Id(), err)
		}
		wp.finishedMutex.Lock()
		wp.finished = append(wp.finished, j)
		wp.finishedMutex.Unlock()
		// fmt.Printf("Job %s finished: %p %p\n", j.Id(),j, wp.finished[len(wp.finished)-1])
	}
	fmt.Printf("Worker %d is shutting down\n", id)
}
