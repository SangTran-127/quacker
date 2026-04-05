package fanout

import (
	"context"
	"testing"
)

func BenchmarkFanOut_RoundRobin(b *testing.B) {
	b.ReportAllocs()

	fo, _ := NewFanOut[int](
		WithWorkerCount(4),
		WithBufferSize(64),
		WithStrategy(RoundRobin),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := make(chan int, 64)
	fo.Run(ctx, input)

	// Drain outputs
	for _, ch := range fo.Outputs() {
		go func() {
			for range ch {
			}
		}()
	}

	for b.Loop() {
		input <- 1
	}

	close(input)
}

func BenchmarkFanOut_Broadcast(b *testing.B) {
	b.ReportAllocs()

	fo, _ := NewFanOut[int](
		WithWorkerCount(4),
		WithBufferSize(64),
		WithStrategy(Broadcast),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := make(chan int, 64)
	fo.Run(ctx, input)

	for _, ch := range fo.Outputs() {
		go func() {
			for range ch {
			}
		}()
	}

	for b.Loop() {
		input <- 1
	}

	close(input)
}
