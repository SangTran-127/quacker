package pipeline

import (
	"context"
	"testing"
)

func BenchmarkMap(b *testing.B) {
	b.ReportAllocs()

	input := make(chan int, 256)

	out := Run(b.Context(), input,
		Map(4, func(_ context.Context, v int) (int, bool) {
			return v * 2, true
		}),
	)

	go func() {
		for range out {
		}
	}()

	for b.Loop() {
		input <- 1
	}

	close(input)
}

func BenchmarkPipeline_Composition(b *testing.B) {
	b.ReportAllocs()

	input := make(chan int, 256)

	out := Run(b.Context(), input,
		Filter(func(v int) bool { return v > 0 }),
		Map(4, func(_ context.Context, v int) (int, bool) {
			return v * 2, true
		}),
		Buffer[int](64),
		Sink(2, func(_ context.Context, v int) {}),
	)

	go func() {
		for range out {
		}
	}()

	for b.Loop() {
		input <- 1
	}

	close(input)
}
