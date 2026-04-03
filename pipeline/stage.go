// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

package pipeline

import (
	"context"
	"sync"
)

// Map applies fn to each item with bounded concurrency.
// fn returns (result, keep) — if keep is false the item is dropped.
// Items may be reordered when concurrency > 1.
func Map[T any](concurrency int, fn func(context.Context, T) (T, bool), opts ...StageOption) Stage[T] {
	if concurrency < 1 {
		panic("pipeline: Map concurrency must be at least 1")
	}
	cfg := applyOpts(opts)

	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T, cfg.bufferSize)
		var wg sync.WaitGroup

		for range concurrency {
			wg.Go(func() {
				for {
					select {
					case v, ok := <-in:
						if !ok {
							return
						}
						result, keep := fn(ctx, v)
						if !keep {
							if cfg.observer != nil {
								cfg.observer.OnDrop()
							}
							continue
						}
						if cfg.observer != nil {
							cfg.observer.OnItem()
						}
						select {
						case out <- result:
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
			if cfg.observer != nil {
				cfg.observer.OnDone()
			}
		}()

		return out
	}
}

// Filter passes through items for which fn returns true.
func Filter[T any](fn func(T) bool, opts ...StageOption) Stage[T] {
	cfg := applyOpts(opts)

	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T, cfg.bufferSize)

		go func() {
			defer close(out)
			defer func() {
				if cfg.observer != nil {
					cfg.observer.OnDone()
				}
			}()

			for {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					if !fn(v) {
						if cfg.observer != nil {
							cfg.observer.OnDrop()
						}
						continue
					}
					if cfg.observer != nil {
						cfg.observer.OnItem()
					}
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		return out
	}
}

// ForEach calls fn on each item for side effects, then forwards it unchanged.
// fn should be lightweight — it runs in a single goroutine.
func ForEach[T any](fn func(T), opts ...StageOption) Stage[T] {
	cfg := applyOpts(opts)

	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T, cfg.bufferSize)

		go func() {
			defer close(out)
			defer func() {
				if cfg.observer != nil {
					cfg.observer.OnDone()
				}
			}()

			for {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					fn(v)
					if cfg.observer != nil {
						cfg.observer.OnItem()
					}
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		return out
	}
}

// Buffer inserts a buffered channel between stages.
// This decouples producer and consumer timing — the upstream can get
// ahead by up to size items before backpressure kicks in.
func Buffer[T any](size int) Stage[T] {
	if size < 1 {
		panic("pipeline: Buffer size must be at least 1")
	}

	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T, size)

		go func() {
			defer close(out)
			for {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		return out
	}
}

// Tee sends each item to a side channel (for logging, metrics, auditing)
// while forwarding it to the next stage. Both sends respect ctx.Done.
// If the side channel is full, the pipeline blocks — size the side
// channel buffer to match your throughput needs.
func Tee[T any](side chan<- T, opts ...StageOption) Stage[T] {
	if side == nil {
		panic("pipeline: Tee side channel must not be nil")
	}
	cfg := applyOpts(opts)

	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T, cfg.bufferSize)

		go func() {
			defer close(out)
			defer func() {
				if cfg.observer != nil {
					cfg.observer.OnDone()
				}
			}()

			for {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					if cfg.observer != nil {
						cfg.observer.OnItem()
					}
					select {
					case side <- v:
					case <-ctx.Done():
						return
					}
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		return out
	}
}

// Take forwards the first n items, then closes the output.
// Upstream goroutines remain alive until the context is cancelled —
// the caller should cancel ctx after consuming Take's output.
func Take[T any](n int, opts ...StageOption) Stage[T] {
	if n < 0 {
		panic("pipeline: Take count must not be negative")
	}
	cfg := applyOpts(opts)

	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T, cfg.bufferSize)

		go func() {
			defer close(out)
			defer func() {
				if cfg.observer != nil {
					cfg.observer.OnDone()
				}
			}()

			count := 0
			for count < n {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					if cfg.observer != nil {
						cfg.observer.OnItem()
					}
					select {
					case out <- v:
						count++
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		return out
	}
}

// Sink consumes items with bounded concurrency. It is a terminal stage —
// no items are forwarded. The returned channel closes when all items
// are processed, so the caller can wait with: for range out {}
func Sink[T any](concurrency int, fn func(context.Context, T), opts ...StageOption) Stage[T] {
	if concurrency < 1 {
		panic("pipeline: Sink concurrency must be at least 1")
	}
	cfg := applyOpts(opts)

	return func(ctx context.Context, in <-chan T) <-chan T {
		done := make(chan T) // never written to, closed when workers finish
		var wg sync.WaitGroup

		for range concurrency {
			wg.Go(func() {
				for {
					select {
					case v, ok := <-in:
						if !ok {
							return
						}
						fn(ctx, v)
						if cfg.observer != nil {
							cfg.observer.OnItem()
						}
					case <-ctx.Done():
						return
					}
				}
			})
		}

		go func() {
			wg.Wait()
			close(done)
			if cfg.observer != nil {
				cfg.observer.OnDone()
			}
		}()

		return done
	}
}
