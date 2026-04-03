// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

package pipeline

import (
	"context"
	"time"
)

// Batch collects items into slices of up to size, flushing when the batch
// is full or when timeout elapses (whichever comes first).
//
// This is a standalone function (not a Stage[T]) because it changes the
// element type from T to []T. Use it between two Run calls:
//
//	mapped := pipeline.Run(ctx, input, pipeline.Map(3, transform))
//	batched := pipeline.Batch(ctx, mapped, 100, time.Second)
//	out := pipeline.Run(ctx, batched, pipeline.Sink(5, storeBatch))
func Batch[T any](ctx context.Context, in <-chan T, size int, timeout time.Duration) <-chan []T {
	if in == nil {
		panic("pipeline: Batch input channel must not be nil")
	}
	if size < 1 {
		panic("pipeline: Batch size must be at least 1")
	}

	out := make(chan []T)

	go func() {
		defer close(out)

		buf := make([]T, 0, size)
		ticker := time.NewTicker(timeout)
		defer ticker.Stop()

		flush := func() bool {
			if len(buf) == 0 {
				return true
			}
			batch := buf
			buf = make([]T, 0, size)
			select {
			case out <- batch:
				return true
			case <-ctx.Done():
				return false
			}
		}

		for {
			select {
			case v, ok := <-in:
				if !ok {
					flush()
					return
				}
				buf = append(buf, v)
				if len(buf) >= size {
					if !flush() {
						return
					}
				}
			case <-ticker.C:
				if !flush() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
