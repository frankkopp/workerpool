package WorkerPool

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// WorkPackage todo
type WorkPackage struct {
	jobNumber int
	f         float64
	div       float64
	result    time.Duration
}

func (w *WorkPackage) Id() string {
	return strconv.Itoa(w.jobNumber)
}

func (w *WorkPackage) Run() error {
	startTime := time.Now()
	// simulate cpu intense calculation
	f := w.f
	for f > 1 {
		f /= w.div
	}
	w.result = time.Since(startTime)
	return nil
}

func TestWorkerPool(t *testing.T) {
	size := 4

	pool := NewWorkerPool(4, size)

	fmt.Printf("Sending work to WorkPool\n")
	for j := 1; j <= size; j++ {
		wp := &WorkPackage{
			jobNumber: j,
			f:         10000000.0,
			div:       1.0000001,
			result:    0,
		}
		pool.QueueJob(wp)
	}

	go func() {
		time.Sleep(10 * time.Second)
		pool.Close()
	}()

	count := 0
	fmt.Printf("Getting finished from pool\n")
	for {
		f, done := pool.GetFinished()
		if done {
			break
		}
		if f != nil {
			finished := f.(*WorkPackage)
			fmt.Printf("Result %s\n", finished.result)
			count++
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
	fmt.Println(count, "Results")
	assert.EqualValues(t, size, count)
}
