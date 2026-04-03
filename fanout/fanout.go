// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

// Package fanout provides a generic fan-out concurrency pattern implementation.
// It distributes values from a single input channel to multiple output channels,
// enabling parallel processing of a single data stream by multiple consumers.
//
// The fan-out pattern is useful when you need to distribute work across multiple
// workers, broadcast events to multiple subscribers, or load-balance tasks
// across a pool of consumers.
//
// Unlike FanIn (which spawns N goroutines to read N inputs concurrently),
// FanOut only spawns ONE dispatcher goroutine to distribute values.
// Therefore, FanOut does not need an internal WaitGroup.
//
// # Distribution Strategies
//
// FanOut supports two distribution strategies:
//
//   - RoundRobin: Distributes values evenly across outputs in a rotating fashion.
//     Each value goes to exactly one output channel, cycling through all workers.
//
//   - BroadCast: Sends each value to ALL output channels simultaneously.
//     All consumers receive every value from the input.
//
// # Consumer Responsibility
//
// Users are responsible for:
//   - Spawning consumer goroutines for each output channel
//   - Managing their own WaitGroup to track consumer completion
//
// # Example Usage
//
// Basic round-robin distribution:
//
//	fo, _ := fanout.NewFanOut[int](
//	    fanout.WithWorkerCount(3),
//	    fanout.WithBufferSize(10),
//	)
//	fo.Run(ctx, input)
//	outputs := fo.Outputs()
//
//	var wg sync.WaitGroup
//	for i, ch := range outputs {
//	    wg.Add(1)
//	    go func(id int, jobs <-chan int) {
//	        defer wg.Done()
//	        for job := range jobs {
//	            process(id, job)
//	        }
//	    }(i, ch)
//	}
//	wg.Wait()
//
// Broadcast to all consumers:
//
//	fo, _ := fanout.NewFanOut[Event](
//	    fanout.WithWorkerCount(5),
//	    fanout.WithStrategy(fanout.BroadCast),
//	)
//	fo.Run(ctx, eventStream)
package fanout

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// FanOutObserver defines callbacks for monitoring fan-out lifecycle events.
// Implementations can use these hooks for logging, metrics collection,
// or coordinating with external systems.
type FanOutObserver interface {
	// OnDistribute is called each time a value is successfully sent to an output channel.
	// In RoundRobin mode, this is called once per value.
	// In BroadCast mode, this is called once per output channel per value.
	OnDistribute()
	// OnOutputClosed is called when all output channels have been closed.
	// This indicates that the fan-out operation has completed.
	OnOutputClosed()
}

// FanOutStrategy defines how values are distributed across output channels.
type FanOutStrategy int

const (
	// RoundRobin distributes values evenly across output channels in a rotating fashion.
	// Each value is sent to exactly one output channel, cycling through workers sequentially.
	// Use this strategy for load balancing work across multiple consumers.
	RoundRobin FanOutStrategy = iota
	// Broadcast sends each value to ALL output channels simultaneously.
	// Every consumer receives every value from the input stream.
	// Use this strategy for event broadcasting or pub/sub patterns.
	Broadcast
)

// ErrInvalidStrategy is returned when an unknown FanOutStrategy is provided.
var ErrInvalidStrategy = errors.New("fanout: unknown distribution strategy")

// FanOutConfig holds configuration options for creating a FanOut instance.
type FanOutConfig struct {
	// WorkerCount specifies the number of output channels to create.
	// Must be at least 1. Default is 1.
	WorkerCount int
	// BufferSize specifies the capacity of each output channel buffer.
	// A value of 0 creates unbuffered channels. Must be non-negative. Default is 0.
	BufferSize int
	// Strategy determines how values are distributed across outputs.
	// Default is RoundRobin.
	Strategy FanOutStrategy
	// Observer receives callbacks for fan-out lifecycle events.
	// Can be nil if no observation is needed.
	Observer FanOutObserver
}

// Option is a functional option for configuring a FanOut instance.
// Options are passed to NewFanOut to customize behavior.
type Option func(*FanOutConfig)

// FanOut distributes values from a single input channel to multiple output channels.
// It is generic and can distribute any type T across configured output channels.
//
// FanOut respects context cancellation and will gracefully shut down when
// the context is cancelled. All output channels are closed when the input
// channel is closed or the context is cancelled.
//
// FanOut is safe for concurrent access to Outputs(), but Run() must only
// be called once per instance.
type FanOut[T any] struct {
	cfg           *FanOutConfig
	outputs       []chan T
	running       atomic.Bool
	cachedOutputs []<-chan T
	once          sync.Once
}

// NewFanOut creates a new FanOut instance with the provided options.
// It returns an error if the configuration is invalid (e.g., WorkerCount < 1
// or BufferSize < 0).
//
// Options can be used to configure the number of workers, buffer size,
// distribution strategy, and attach an observer:
//
//	fo, err := fanout.NewFanOut[string](
//	    fanout.WithWorkerCount(4),
//	    fanout.WithBufferSize(100),
//	    fanout.WithStrategy(fanout.RoundRobin),
//	    fanout.WithObserver(myObserver),
//	)
func NewFanOut[T any](opts ...Option) (*FanOut[T], error) {
	cfg := &FanOutConfig{
		WorkerCount: 1,
		BufferSize:  0,
		Observer:    nil,
		Strategy:    RoundRobin,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.WorkerCount < 1 {
		return nil, fmt.Errorf("fanout: worker count must be at least 1, got %d", cfg.WorkerCount)
	}

	if cfg.BufferSize < 0 {
		return nil, fmt.Errorf("fanout: buffer size must not be negative, got %d", cfg.BufferSize)
	}

	if cfg.Strategy != RoundRobin && cfg.Strategy != Broadcast {
		return nil, ErrInvalidStrategy
	}

	outputs := make([]chan T, cfg.WorkerCount)

	for i := range outputs {
		outputs[i] = make(chan T, cfg.BufferSize)
	}

	return &FanOut[T]{
		cfg:     cfg,
		outputs: outputs,
	}, nil
}

// Run starts the fan-out distribution from the input channel to all output channels.
// It spawns a single dispatcher goroutine that reads from the input and distributes
// values according to the configured strategy (RoundRobin or Broadcast).
//
// Run returns immediately after starting the dispatcher goroutine. The dispatcher
// continues running until either:
//   - The input channel is closed
//   - The context is cancelled
//
// When the dispatcher stops, all output channels are automatically closed.
//
// Run can only be called once per FanOut instance. Calling Run multiple times
// will panic. This ensures clear ownership semantics and prevents race conditions.
//
// Example:
//
//	input := make(chan int)
//	go func() {
//	    for i := 0; i < 100; i++ {
//	        input <- i
//	    }
//	    close(input)
//	}()
//	fo.Run(ctx, input)
func (f *FanOut[T]) Run(ctx context.Context, input <-chan T) {
	if input == nil {
		panic("fanout: input channel must not be nil")
	}

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

				case Broadcast:
					f.broadcast(ctx, value)
				}

			case <-ctx.Done():
				return
			}
		}
	}()
}

// Outputs returns the output channels that receive distributed values.
// The returned slice contains receive-only channel references, preventing
// consumers from accidentally closing or sending to these channels.
//
// The number of channels equals the WorkerCount configured during creation.
// This method is safe to call concurrently and will always return the same
// slice of channels.
//
// Consumers should spawn goroutines to read from these channels:
//
//	for i, ch := range fo.Outputs() {
//	    go func(id int, jobs <-chan T) {
//	        for job := range jobs {
//	            process(id, job)
//	        }
//	    }(i, ch)
//	}
func (f *FanOut[T]) Outputs() []<-chan T {
	f.once.Do(func() {
		// Return receive-only channel references to prevent consumers from closing or sending to these channels.
		f.cachedOutputs = make([]<-chan T, len(f.outputs))

		for i, ch := range f.outputs {
			f.cachedOutputs[i] = ch
		}
	})

	return f.cachedOutputs
}

// roundRobin distributes a value to the next output channel in rotation.
// It cycles through outputs sequentially, ensuring even distribution of work.
// The operation respects context cancellation for graceful shutdown.
func (f *FanOut[T]) roundRobin(ctx context.Context, value T, index *int) {
	target := *index % f.cfg.WorkerCount
	select {
	case f.outputs[target] <- value:
		*index = (*index + 1) % f.cfg.WorkerCount
		if f.cfg.Observer != nil {
			f.cfg.Observer.OnDistribute()
		}
	case <-ctx.Done():
		return
	}
}

// broadcast sends a value to ALL output channels simultaneously.
// It spawns a goroutine for each output to prevent slow consumers from
// blocking delivery to other outputs. The method waits for all sends
// to complete before returning.
// The operation respects context cancellation for graceful shutdown.
func (f *FanOut[T]) broadcast(ctx context.Context, value T) {
	var wg sync.WaitGroup
	for _, ch := range f.outputs {
		// Create goroutine to prevent bottleneck if any consumer is slow
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

// closeAllOutput closes all output channels and notifies the observer.
// This is called when the dispatcher goroutine exits, signaling to consumers
// that no more values will be sent.
func (f *FanOut[T]) closeAllOutput() {
	for _, ch := range f.outputs {
		close(ch)
	}

	if f.cfg.Observer != nil {
		f.cfg.Observer.OnOutputClosed()
	}
}

// WithWorkerCount sets the number of output channels (workers) for the fan-out.
// Each output channel can be consumed by a separate goroutine for parallel processing.
// Must be at least 1. Default is 1.
//
// Returns an Option that can be passed to NewFanOut.
func WithWorkerCount(n int) Option {
	return func(cfg *FanOutConfig) {
		cfg.WorkerCount = n
	}
}

// WithBufferSize sets the buffer capacity for each output channel.
// A size of 0 creates unbuffered channels (default). Larger values allow
// the dispatcher to continue sending without blocking until buffers fill,
// which can improve throughput when consumers have variable processing times.
//
// Returns an Option that can be passed to NewFanOut.
func WithBufferSize(size int) Option {
	return func(cfg *FanOutConfig) {
		cfg.BufferSize = size
	}
}

// WithStrategy sets the distribution strategy for the fan-out.
// Available strategies:
//   - RoundRobin (default): Each value goes to one output in rotation
//   - BroadCast: Each value goes to ALL outputs
//
// Returns an Option that can be passed to NewFanOut.
func WithStrategy(strategy FanOutStrategy) Option {
	return func(cfg *FanOutConfig) {
		cfg.Strategy = strategy
	}
}

// WithObserver attaches a FanOutObserver to receive lifecycle event callbacks.
// The observer's methods are called synchronously when values are distributed
// or when output channels are closed. Pass nil to disable observation (default).
//
// Returns an Option that can be passed to NewFanOut.
func WithObserver(observer FanOutObserver) Option {
	return func(cfg *FanOutConfig) {
		cfg.Observer = observer
	}
}
