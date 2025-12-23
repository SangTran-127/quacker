package fanin

import (
	"context"
	"sync"
)

type FanIn[T any] struct {
	inputs []chan T
	output chan T
	wg     sync.WaitGroup
	ctx    context.Context
	// Size for buffer channel, apply for throughput
	bufferSize int
}

type FanInConfig struct {
	BufferSize int
}

func NewFanIn[T any](context context.Context, cfg *FanInConfig) (*FanIn[T], error) {

	
	return &FanIn[T]{
		output: make(chan T, cfg.BufferSize),
	}, nil
}

// func Fann[T any](inputs ...<-chan T) <-chan T {
// 	var wg sync.WaitGroup
// 	out := make(chan T)

// 	wg.Add(len(inputs))
// 	for _, ch := range inputs {
// 		go func(c <-chan T) {
// 			defer wg.Done()
// 			for v := range c {
// 				out <- v
// 			}
// 		}(ch)
// 	}

// 	go func() {
// 		wg.Wait()
// 		close(out)
// 	}()

// 	return out
// }
