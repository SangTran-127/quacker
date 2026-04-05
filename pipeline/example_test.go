package pipeline_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/SangTran-127/quacker/pipeline"
)

func ExampleRun() {
	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	out := pipeline.Run(context.Background(), input,
		pipeline.Filter(func(v int) bool { return v%2 == 0 }),
		pipeline.Map(1, func(_ context.Context, v int) (int, bool) {
			return v * 10, true
		}),
	)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	sort.Ints(results)
	fmt.Println(results)
	// Output: [20 40]
}

func ExampleSink() {
	input := make(chan string, 3)
	input <- "hello"
	input <- "world"
	input <- "!"
	close(input)

	var collected []string
	out := pipeline.Run(context.Background(), input,
		pipeline.Sink(1, func(_ context.Context, v string) {
			collected = append(collected, v)
		}),
	)

	for range out {
	}

	fmt.Println(collected)
	// Output: [hello world !]
}
