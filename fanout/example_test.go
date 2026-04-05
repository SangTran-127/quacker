package fanout_test

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/SangTran-127/quacker/fanout"
)

func ExampleNewFanOut() {
	fo, _ := fanout.NewFanOut[int](
		fanout.WithWorkerCount(2),
		fanout.WithBufferSize(10),
		fanout.WithStrategy(fanout.RoundRobin),
	)

	input := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		input <- i
	}
	close(input)

	fo.Run(context.Background(), input)

	var mu sync.Mutex
	var all []int
	var wg sync.WaitGroup

	for _, ch := range fo.Outputs() {
		wg.Go(func() {
			for v := range ch {
				mu.Lock()
				all = append(all, v)
				mu.Unlock()
			}
		})
	}

	wg.Wait()
	sort.Ints(all)
	fmt.Println(all)
	// Output: [1 2 3 4]
}
