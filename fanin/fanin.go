// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

package fanin

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type FanInObserver interface {
	OnInputAdded()
	OnInputClosed()
}

type FanInConfig struct {
	BufferSize int
	Observer   FanInObserver
}

type Option func(*FanInConfig)

type FanIn[T any] struct {
	cfg     *FanInConfig
	mu      sync.Mutex
	inputs  []<-chan T
	running atomic.Bool
}

func NewFanIn[T any](opts ...Option) (*FanIn[T], error) {

	cfg := &FanInConfig{
		BufferSize: 0,
	}

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

func (f *FanIn[T]) Add(ch <-chan T) error {
	if f.running.Load() {
		return fmt.Errorf("fanin: Add() called after Run()")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.inputs = append(f.inputs, ch)

	if f.cfg.Observer != nil {
		f.cfg.Observer.OnInputAdded()
	}

	return nil
}

func (f *FanIn[T]) Run(ctx context.Context) <-chan T {
	// Atomic already thread-safe, no need lock
	if !f.running.CompareAndSwap(false, true) {
		panic("fanin: Run() called multiple times")
	}

	f.mu.Lock()
	// Immutable snapshot the channel slices, prevent data races
	// Because inputs is shared mutable array
	inputs := append([]<-chan T(nil), f.inputs...)
	f.mu.Unlock()
	// Don't store out(chan T) in FanIn struct
	// Follow these rules:
	// Whoever creates the channel is the one who closes it.
	// If store in struct, we can't Run(ctx) again because once it close
	// We cannot call it again
	out := make(chan T, f.cfg.BufferSize)
	var wg sync.WaitGroup
	wg.Add(len(inputs))
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
	}()

	return out
}
