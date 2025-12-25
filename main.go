package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/SangTran-127/quacker/fanin"
)

func WithBuffer(size int) fanin.Option {
	return func(c *fanin.FanInConfig) {
		c.BufferSize = size
	}
}

func main() {

	ctx := context.Background()
	realctx, _ := context.WithTimeout(ctx, 2*time.Second)
	fanin, err := fanin.NewFanIn[string](WithBuffer(4))

	if err != nil {
		log.Fatal(err)
	}

	ch1 := make(chan string, 1)
	ch2 := make(chan string, 1)
	ch3 := make(chan string, 1)
	ch4 := make(chan string, 1)

	sliceCh := []chan string{ch1, ch2, ch3, ch4}

	for i := range 4 {
		sliceCh[i] <- fmt.Sprintf("%d", rand.Int())
		fanin.Add(sliceCh[i])
	}

	for v := range fanin.Run(realctx) {
		fmt.Println(v)
	}

}
