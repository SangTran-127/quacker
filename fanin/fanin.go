// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

// Package fanin provides a generic fan-in concurrency pattern implementation.
// It merges multiple input channels into a single output channel, allowing
// concurrent producers to feed into a unified consumer stream.
//
// The fan-in pattern is useful when you have multiple goroutines producing
// values that need to be processed by a single consumer, such as aggregating
// results from parallel workers or combining multiple event streams.
//
// Example usage:
//
//	fanIn, _ := fanin.NewFanIn[int]()
//	fanIn.Add(producer1)
//	fanIn.Add(producer2)
//	out := fanIn.Run(ctx)
//	for v := range out {
//	    // process merged values
//	}
package fanin

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// FanInObserver defines callbacks for monitoring fan-in lifecycle events.
// Implementations can use these hooks for logging, metrics collection,
// or coordinating with external systems.
type FanInObserver interface {
	// OnInputAdded is called each time a new input channel is registered via Add().
	OnInputAdded()
	// OnInputClosed is called when an input channel is closed by its producer.
	OnInputClosed()
}

// FanInConfig holds configuration options for creating a FanIn instance.
type FanInConfig struct {
	// BufferSize specifies the capacity of the output channel buffer.
	// A value of 0 creates an unbuffered channel. Must be non-negative.
	BufferSize int
	// Observer receives callbacks for fan-in lifecycle events.
	// Can be nil if no observation is needed.
	Observer FanInObserver
}

// Option is a functional option for configuring a FanIn instance.
// Options are passed to NewFanIn to customize behavior.
type Option func(*FanInConfig)

// FanIn merges multiple input channels of type T into a single output channel.
// It is safe for concurrent use; multiple goroutines can call Add concurrently
// before Run is called. Once Run is called, no more inputs can be added.
//
// FanIn respects context cancellation and will gracefully shut down all
// internal goroutines when the context is cancelled.
type FanIn[T any] struct {
	cfg     *FanInConfig
	mu      sync.Mutex
	inputs  []<-chan T
	running atomic.Bool
	done    chan struct{}
}

// NewFanIn creates a new FanIn instance with the provided options.
// It returns an error if the configuration is invalid (e.g., negative buffer size).
//
// Options can be used to configure buffer size and attach an observer:
//
//	fanIn, err := fanin.NewFanIn[string](
//	    fanin.WithBufferSize(100),
//	    fanin.WithObserver(myObserver),
//	)
func NewFanIn[T any](opts ...Option) (*FanIn[T], error) {
	cfg := &FanInConfig{}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.BufferSize < 0 {
		return nil, fmt.Errorf("fanin: buffer size must be >= 0, got %d", cfg.BufferSize)
	}

	return &FanIn[T]{
		cfg: cfg,
	}, nil
}

// Add registers an input channel to be merged into the fan-in output.
// It must be called before Run; calling Add after Add will panic if invoked.
//
// Add is safe for concurrent use from multiple goroutines.
// The channel will be read until it is closed or the context passed to Run is cancelled.
//
// It panics if called after Run has been invoked.
func (f *FanIn[T]) Add(ch <-chan T) {
	if f.running.Load() {
		panic("fanin: Add() called after Run()")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.inputs = append(f.inputs, ch)

	if f.cfg.Observer != nil {
		f.cfg.Observer.OnInputAdded()
	}

}

// Run starts the fan-in operation and returns the merged output channel.
// It spawns a goroutine for each registered input channel to forward values
// to the output channel. The output channel is closed when all input channels
// are closed or the context is cancelled.
//
// Run can only be called once per FanIn instance. Calling Run multiple times
// will panic. After Run is called, Add will return an error.
// To keep simplify & clear ownership => 1 instance = 1 lifecycle = 1 output channel
//
// The returned channel should be consumed by the caller. When the context is
// cancelled, all internal goroutines will exit gracefully without leaking.
func (f *FanIn[T]) Run(ctx context.Context) <-chan T {
	if !f.running.CompareAndSwap(false, true) {
		panic("fanin: Run() called multiple times")
	}

	f.mu.Lock()
	// Immutable snapshot the channel slices, prevent data races
	// Because inputs is shared mutable array
	inputs := append([]<-chan T(nil), f.inputs...)
	// Don't store out(chan T) in FanIn struct
	// Follow these rules:
	// Whoever creates the channel is the one who closes it.
	// If store in struct, we can't Run(ctx) again because once it close
	// We cannot call it again
	out := make(chan T, f.cfg.BufferSize)
	f.done = make(chan struct{})
	f.mu.Unlock()
	var wg sync.WaitGroup
	for _, ch := range inputs {
		wg.Go(func() {
			for {
				select {
				case v, ok := <-ch:
					if !ok {
						if f.cfg.Observer != nil {
							f.cfg.Observer.OnInputClosed()
						}
						return
					}
					select {
					// Should use select because if ctx cancelled
					// while a goroutine is blocked on out <- v
					// the goroutine wont exit
					// Cause goroutine leak
					case out <- v:
					case <-ctx.Done():
						return

					}
				case <-ctx.Done():
					return

				}
			}

		})
	}

	go func() {
		wg.Wait()
		close(out)
		close(f.done)
	}()

	return out
}

// Done returns a channel that is closed when the fan-in operation completes.
// This occurs when all input channels have been closed and drained, or when
// the context passed to Run is cancelled.
//
// Returns nil if called before Run(). Receiving from a nil channel blocks forever,
// so callers should ensure Run() has been called before using Done().
//
// Done can be used to wait for the fan-in to finish:
//
//	out := fanIn.Run(ctx)
//	// ... consume out ...
//	<-fanIn.Done()
//	fmt.Println("all inputs processed")
func (f *FanIn[T]) Done() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.done
}
