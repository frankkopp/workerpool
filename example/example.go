// Example for package github.com/frankkopp/workerpool

package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/frankkopp/workerpool"
)

// WorkPackage todo
type WorkPackage struct {
	JobID  int
	Result time.Duration
	f      float64
	div    float64
}

// Id identification of the work package
func (w *WorkPackage) Id() string {
	return strconv.Itoa(w.JobID)
}

// Run will be executed by workerpool
func (w *WorkPackage) Run() error {
	startTime := time.Now()
		fmt.Println("Working...")
	// simulate cpu intense calculation
	f := w.f
	for f > 1 {
		f /= w.div
	}
	// simulate a Result to be stored in the struct
	w.Result = time.Since(startTime)
	return nil
}

func main() {
	noOfWorkers := 2
	bufferSize := 5
	pool := workerpool.NewWorkerPool(noOfWorkers, bufferSize, true)

	// Timed stop routine
	go func() {
		time.Sleep(6500 * time.Millisecond)
		fmt.Println("Stop =======================")
		err := pool.Stop()
		if err != nil {
			fmt.Println(err)
		}
	}()

	// Timed retrieval routine
	go func() {
		time.Sleep(5 * time.Second)
		for i := 0; ; {
			getFinishedWait, done := pool.GetFinishedWait()
			if done {
				fmt.Println("WorkerPool finished queue closed")
				break
			}
			if getFinishedWait != nil {
				i++
				fmt.Println("Waiting : ", pool.Jobs())
				fmt.Println("Working : ", pool.RunningJobs())
				fmt.Println("Finished: ", pool.FinishedJobs())
				fmt.Println("Received: ", i)
				fmt.Println("Result  : ", getFinishedWait.(*WorkPackage).Result, " === ")
				fmt.Println()
			}
		}
		fmt.Println()
	}()

	// Adding jobs
	for i := 1; i <= 25; i++ {
		job := &WorkPackage{
			JobID:  i,
			f:      10000000.0,
			div:    1.0000001,
			Result: 0,
		}
		err := pool.QueueJob(job)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Added: ", i, "Waiting: ", pool.WaitingJobs())
	}
	fmt.Println()

	// Close queue
	fmt.Println("Close Queue")
	err := pool.Close()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println()

	// Try adding to closed queue
	for i := 0; i < 10; i++ {
		job := &WorkPackage{
			JobID:  i + 10,
			f:      10000000.0,
			div:    1.0000001,
			Result: 0,
		}
		err := pool.QueueJob(job)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
	fmt.Println("Waiting: ", pool.WaitingJobs())
	fmt.Println()

	// Try closing a second time
	fmt.Println("Close Queue second time")
	err = pool.Close()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println()

}
