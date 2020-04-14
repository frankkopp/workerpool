/*
 * MIT License
 *
 * Copyright (c) 2020 Frank Kopp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

// Example for package github.com/frankkopp/workerpool

package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/frankkopp/workerpool"
)

// WorkPackage for test
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

	// Timed stop routine - tells the workerpool to stop after some time
	// The time should be longer than the retriever delay below for this
	// example
	go func() {
		time.Sleep(6500 * time.Millisecond)
		fmt.Println("Stop =======================")
		err := pool.Stop()
		if err != nil {
			fmt.Println(err)
		}
	}()

	// Timed retrieval routine - starts retrieving results after some time
	go func() {
		time.Sleep(5 * time.Second)
		for consumed := 0; ; {
			getFinishedWait, done := pool.GetFinishedWait()
			if done {
				fmt.Println("WorkerPool finished queue closed")
				break
			}
			if getFinishedWait != nil {
				consumed++
				fmt.Println("Waiting : ", pool.Jobs())
				fmt.Println("Working : ", pool.RunningJobs())
				fmt.Println("Finished: ", pool.FinishedJobs())
				fmt.Println("Received: ", consumed)
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

	// Close queue - this will close the workerpool which prevents adding
	// new jobs but will finish any waiting and running jobs.
	fmt.Println("Close Queue ==============")
	err := pool.Close()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println()

	// Try adding to closed queue - this will be rejected and an error
	// will be raised
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
	fmt.Println()
}
