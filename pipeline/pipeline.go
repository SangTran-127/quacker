// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

// Package pipeline provides composable, type-safe stream processing with
// backpressure for Go. Stages connect via channels — backpressure propagates
// naturally through Go's channel blocking semantics.
//
// A Stage transforms a receive-only input channel into a receive-only output
// channel. Stages compose via Run, which wires them together like Unix pipes.
//
// Backpressure: when a downstream stage stops reading, its input channel fills,
// which blocks the upstream stage's send, which stops it from reading its own
// input — cascading all the way back to the source. Buffer sizes control how
// much burst each stage absorbs before backpressure kicks in.
//
// Shutdown: close the input channel for graceful drain (all inflight items
// processed). Cancel the context for immediate stop (goroutines exit via
// ctx.Done, channels close in cascade).
//
// Errors: stages do not impose an error model. Use context.WithCancelCause
// in your stage functions — the first error cancels the pipeline, and the
// caller retrieves it with context.Cause(ctx).
//
// Example:
//
//	ctx, cancel := context.WithCancelCause(ctx)
//	out := pipeline.Run(ctx, input,
//	    pipeline.Map(3, enrich),
//	    pipeline.Filter(validate),
//	    pipeline.Sink(5, store),
//	)
//	for range out {}
//	if err := context.Cause(ctx); err != nil {
//	    log.Fatal(err)
//	}
package pipeline

import "context"

// Stage transforms an input channel into an output channel.
// The stage owns the output channel and must close it when the input
// is exhausted or the context is cancelled.
type Stage[T any] func(ctx context.Context, in <-chan T) <-chan T

// Run connects stages into a pipeline and returns the final output channel.
// Each stage's output becomes the next stage's input.
// With zero stages, the input channel is returned directly.
func Run[T any](ctx context.Context, input <-chan T, stages ...Stage[T]) <-chan T {
	if input == nil {
		panic("pipeline: input channel must not be nil")
	}
	ch := input
	for _, stage := range stages {
		ch = stage(ctx, ch)
	}
	return ch
}
