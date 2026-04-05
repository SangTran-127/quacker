package fanin_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/SangTran-127/quacker/fanin"
)

func ExampleNewFanIn() {
	fi, _ := fanin.NewFanIn[int]()

	ch1 := make(chan int, 2)
	ch1 <- 1
	ch1 <- 2
	close(ch1)

	ch2 := make(chan int, 2)
	ch2 <- 3
	ch2 <- 4
	close(ch2)

	fi.Add(ch1)
	fi.Add(ch2)

	var results []int
	for v := range fi.Run(context.Background()) {
		results = append(results, v)
	}

	sort.Ints(results)
	fmt.Println(results)
	// Output: [1 2 3 4]
}
