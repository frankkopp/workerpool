# WorkerPool
A WorkerPool Implementation in GO

A common problem of parallel computing in high performance applications is the cost of starting new parallel threads.
Although GO is very effective and fast when it comes to start new go routines it still might be too expensive in some cases.
A good approach is usually to have a pool of workers which run separately in different threads (or go routines). Starting a 
new parallel computation is then usually just a matter of queuing a new work job for one of the workers to pick up. 

One of the problems with a ThreadPool in Go I'd like to solve is to use Go channels for queuing and retrieving work and 
results.

This Worker Pool shall fulfill the following requirements.

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
    * be stoppable (skipp all remaining queued jobs) - TODO/TEST
* Allow shutdown - OK
    * Stop the WorkerPool - OK
    * prevent further reading from the finished queue - OK
    * wake (unblock) already waiting readers - TODO/TEST
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
        * wait until a finished job becomes available if the WorkPool is still able to produce finished jobs - TODO/TEST
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
* avoid busy polling loops
    * TODO: ok for processing - not OK for stop and close - needed loop there
* how to interrupt / wake up a read from a channel?
    * not possible - use select or dummy signal (nil)
* WaitGroup does not give access to its counter - so a separate counter is necessary to see how many workers are still running
    * no better solution found yet
* how to interrupt or even kill a go routine which is running
    * only with flags or channels - not found any other way yet
* how to address a specific go routine - like a pointer to a thread in C++
    * not found a way yet 
* Using Interface for Jobs. It is very confusing to work with pointers and Interfaces.
    * works but still needs lots of careful attention  
    




  
