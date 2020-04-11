# WorkerPool
A WorkerPool Implementation in GO

A common problem of parallel computing in high performance applications is the cost of starting new parallel threads.
Althoug GO is very effective and fast when it comes to start new go routines it still might be too expensive in some cases.
A good approach is usually to have a pool of workers which run separatly in different threads (or go routines). Starting a 
new parallel computation is the usually just a matter of queuing a new work job for one of the workers to pick up. 

One of the problems with a ThreadPool in Go I'd like to solve is to use Go channels for queuing and retrieving work and 
results.
