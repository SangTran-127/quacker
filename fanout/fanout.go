// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

// FanOut distributes values from a single input channel to multiple output channels.
// Unlike FanIn (which spawns N goroutines to read N inputs concurrently),
// FanOut only spawns ONE dispatcher goroutine to distribute values.
// Therefore, FanOut does not need an internal WaitGroup.
//
// Users are responsible for:
//   - Spawning consumer goroutines for each output channel
//   - Managing their own WaitGroup to track consumer completion
//
// Error Handling:
//   - FanOut follows a fail-fast philosophy with no panic recovery
//   - Run() panics if called multiple times (programmer error)
//   - Any panic in the dispatcher goroutine will crash the program
//   - This ensures correct behavior and prevents silent corruption
//
// Example:
//
//	fo.Run(ctx, input)
//	outputs := fo.Outputs()
//
//	var wg sync.WaitGroup
//	for i, ch := range outputs {
//	    wg.Add(1)
//	    go func(id int, jobs <-chan T) {
//	        defer wg.Done()
//	        for job := range jobs {
//	            process(id, job)
//	        }
//	    }(i, ch)
//	}
//	wg.Wait()
package fanout

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type FanOutObserver interface {
	OnDistribute()
	OnOutputClosed()
}

type FanOutStrategy int

const (
	RoundRobin FanOutStrategy = iota
	BroadCast
)

type FanOutConfig struct {
	WorkerSize int
	BufferSize int
	Strategy   FanOutStrategy
	Observer   FanOutObserver
}

type Option func(*FanOutConfig)

type FanOut[T any] struct {
	cfg     *FanOutConfig
	outputs []chan T
	running atomic.Bool
}

func NewFanOut[T any](opts ...Option) (*FanOut[T], error) {
	cfg := &FanOutConfig{
		WorkerSize: 1,
		BufferSize: 0,
		Strategy:   RoundRobin,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.WorkerSize < 1 {
		return nil, fmt.Errorf("fanout: number of workers must greater than 0")
	}

	if cfg.BufferSize < 0 {
		return nil, fmt.Errorf("fanout: buffer size must not be a negative integer")
	}

	outputs := make([]chan T, cfg.WorkerSize)

	for i := range outputs {
		outputs[i] = make(chan T, cfg.BufferSize)
	}

	return &FanOut[T]{
		cfg:     cfg,
		outputs: outputs,
	}, nil
}

// Run starts the dispatcher goroutine that distributes values from input to output channels.
// It panics if called multiple times on the same FanOut instance (programmer error).
// No panic recovery is performed - any panic will crash the program to ensure fail-fast behavior.
func (f *FanOut[T]) Run(ctx context.Context, input <-chan T) {
	if !f.running.CompareAndSwap(false, true) {
		panic("fanout: Run() called multiple times")
	}

	go func() {
		idx := 0
		defer f.running.Store(false)
		defer f.closeAllOutput()
		for {
			select {
			case value, ok := <-input:
				if !ok {
					return
				}
				switch f.cfg.Strategy {
				case RoundRobin:
					f.roundRobin(ctx, value, &idx)

				case BroadCast:
					f.broadcast(ctx, value)
				}

			case <-ctx.Done():
				return
			}
		}
	}()

}

func (f *FanOut[T]) Outputs() []<-chan T {
	// Return a received only reference prev
	outputs := make([]<-chan T, len(f.outputs))

	for i, ch := range f.outputs {
		outputs[i] = ch
	}
	return outputs
}

func (f *FanOut[T]) roundRobin(ctx context.Context, value T, index *int) {
	select {
	case f.outputs[*index%f.cfg.WorkerSize] <- value:
		*index++
		if f.cfg.Observer != nil {
			f.cfg.Observer.OnDistribute()
		}
	case <-ctx.Done():
		return
	}
}

func (f *FanOut[T]) broadcast(ctx context.Context, value T) {
	var wg sync.WaitGroup
	for _, ch := range f.outputs {
		// I create Goroutine to prevent bottleneck if any of them slow
		wg.Go(func() {
			select {
			case ch <- value:
				if f.cfg.Observer != nil {
					f.cfg.Observer.OnDistribute()
				}
			case <-ctx.Done():
				return
			}
		})
	}

	wg.Wait()
}

func (f *FanOut[T]) closeAllOutput() {
	for _, ch := range f.outputs {
		close(ch)
	}

	if f.cfg.Observer != nil {
		f.cfg.Observer.OnOutputClosed()
	}
}

func WithWorkers(n int) Option {
	return func(cfg *FanOutConfig) {
		cfg.WorkerSize = n
	}
}

func WithBufferSize(size int) Option {
	return func(cfg *FanOutConfig) {
		cfg.BufferSize = size
	}
}

func WithStrategy(strategy FanOutStrategy) Option {
	return func(cfg *FanOutConfig) {
		cfg.Strategy = strategy
	}
}

func WithObserver(observer FanOutObserver) Option {
	return func(cfg *FanOutConfig) {
		cfg.Observer = observer
	}
}
