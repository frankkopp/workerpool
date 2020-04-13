# WorkerPool
A WorkerPool Implementation in GO

Status: Version 1.0 in Development

[![Build Status](https://travis-ci.org/frankkopp/WorkerPool.svg?branch=master)](https://travis-ci.org/frankkopp/WorkerPool)
[![Go Report Card](https://goreportcard.com/badge/github.com/frankkopp/WorkerPool)](https://goreportcard.com/report/github.com/frankkopp/WorkerPool)

A common problem of parallel computing in high performance applications is the cost of starting new parallel threads.
Although GO is very effective and fast when it comes to start new go routines it still might be too expensive in some cases.
A good approach is usually to have a pool of workers which run separately in different threads (or go routines). Starting a 
new parallel computation is then usually just a matter of queuing a new work job for one of the workers to pick up. 

One of the problems with a ThreadPool in Go I'd like to solve is to use Go channels for queuing and retrieving work and 
results.

This Worker Pool shall fulfill the requirements listed below. 

## Usage
Work packages (Jobs) need to implement the interface workerpool. Job
 and need to be self-contained. That is errors should be stored within
 the Job instance. The same is true for potential results.
 See folder "example". 

To create a workerpool use:
```
 pool := NewWorkerPool(noOfWorkers, bufferSize, queueFinished)
 noOfWorkers:   are the may number of go routines used
 bufferSize:    the number of jobs which can be queued without
	               blocking the caller. As the workerpool immediately
                starts working on the jobs this number is only
                reached if the computation is slower than the 
                adding of jobs or then the adding and computation 
                is faster than the retrieval of finished jobs
 queueFinished: if this is true finished jobs are send and stored 
                in a finished queue (channel) from which they 
                can and must be retrieved with GetFinished or 
                GetFinishedWait to avoid the buffer to be filled
                which would block the workerpool.
                If this is false the finished jobs are discarded
                and no buffer is used.
```
Adding jobs:
```
err := pool.QueueJob(job)
if err != nil {
    fmt.Println(err)
}
```
Retrieve finished jobs:
```
for {
    getFinishedWait, done := pool.GetFinishedWait()
    if done {
        fmt.Println("WorkerPool finished queue closed")
        break
    }
    if getFinishedWait != nil {
        // do something
    }
}
```
Closing a workerpool will disallow new jobs to be queued but will finish already waiting jobs. 
```
err := pool.Close()
if err != nil {
    fmt.Println(err)
}
```
Stopping a workerpool will disallow new jobs to be queued and will skip any jobs already waiting. 
Running jobs will be finished.  
```
err := pool.Stop()
if err != nil {
    fmt.Println(err)
}
```
Shutdown of a workerpool will stop the workerpool and close the finished jobs queue.
This will prevent access to already finished jobs and any waiting retriever will be
unblocked.  
```
err := pool.Shutdown()
if err != nil {
    fmt.Println(err)
}
```

## Requirements:
* Configurable number of workers - OK
* Configurable size for job queue - OK
    * Non blocking until job queue is full - OK
* Configurable if finished jobs should be queued or ignored - OK
* Stoppable (skip waiting jobs) - OK
    * prevent adding of new jobs - OK
    * completing all jobs already started - OK
    * skipping all jobs not yet started - OK
    * keep finished queue (channel) open  - OK
    * ignore multiple call to stop - OK
* Closable (complete all waiting jobs) - OK
    * prevent adding of new jobs - OK
    * completing all jobs already started - OK
    * start and complete all jobs already in the job queue - OK
    * keep finished queue (channel) open - OK
    * ignore multiple calls to close - OK
    * be stoppable (skipp all remaining queued jobs) - OK
* Allow shutdown - OK
    * Stop the WorkerPool - OK
    * prevent further reading from the finished queue - OK
    * wake (unblock) already waiting readers - OK
* Allow queuing of jobs - OK
    * if the job queue still has capacity return immediately - OK
    * if the job queue is full, block the caller until a slot is free - OK
        * if the job queue is closed, wake/unblock any callers who are blocked and return an error - TODO/TEST
    * If the queue is closed, return immediately with an error - OK
* Allow retrieving of finished jobs - OK
    * Processes can retrieve finished jobs by polling the WorkerPool - OK
    * This can be blocking or non blacking - OK
    * In case of non-blocking the response must either:
        * return a finished job - OK
        * return nil and a signal if the WorkerPool has no chance to ever have another finished job (done) - OK
            * E.g. if the job queue is closed but there are still jobs in progress ==> false
            * E.g. if the job queue is closed and there are no more jobs in progress ==> true
    * In case of blocking:
        * wait until a finished job becomes available if the WorkPool is still able to produce finished jobs - OK
            * E.g. the job queue is not closed
            * E.g. the job queue is closed but there are still jobs in progress
        * unblock and return nil if the job queue is closed and there are no more jobs in progress

### Optional requirements:
* Have counter for jobs waiting, jobs in progress and jobs finished - OK but not always correct
    * the sum of these must always be correct, e.g. equal all queued jobs if no job has been retrieved - TODO

## Definition of a work package (Job)
* Implements an interface to be "runnable" (e.g. has func job.Run())
* The WorkerPool does not need any specific knowledge about the Job apart that it implements the interface
* If the work package has a result it should store the result within the Run() function
* The Result can be stored in the work package struct itself
    * this means we can't make copies of Jobs but need to use pointers to the work package instances
        * this is somewhat tricky with GO and interfaces

## Challenges so far:
* avoid busy polling loops - OK 
    * context.Context WithCancel was the solution.
* how to interrupt / wake up a read from a channel?
    * using select and contect with cancel (Done()) 
* WaitGroup does not give access to its counter - so a separate counter is necessary to see how many workers are still running
    * no better solution found yet
* how to interrupt or even kill a go routine which is running
    * only with flags or channels - not found any other way yet
* how to address a specific go routine - like a pointer to a thread in C++
    * not found a way yet 
* Using Interface for Jobs. It is very confusing to work with pointers and Interfaces.
    * works but still needs lots of careful attention  
    




  
