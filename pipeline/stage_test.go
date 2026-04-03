package pipeline

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMap_Basic(t *testing.T) {
	t.Parallel()

	input := make(chan int, 3)
	input <- 1
	input <- 2
	input <- 3
	close(input)

	out := Map(1, func(_ context.Context, v int) (int, bool) {
		return v * 10, true
	})(t.Context(), input)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	sort.Ints(got)
	if len(got) != 3 || got[0] != 10 || got[1] != 20 || got[2] != 30 {
		t.Fatalf("expected [10 20 30], got %v", got)
	}
}

func TestMap_Concurrent(t *testing.T) {
	t.Parallel()

	input := make(chan int, 100)
	for i := range 100 {
		input <- i
	}
	close(input)

	out := Map(5, func(_ context.Context, v int) (int, bool) {
		return v, true
	})(t.Context(), input)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	if len(got) != 100 {
		t.Fatalf("expected 100 items, got %d", len(got))
	}

	// Verify all items arrived (order may differ with concurrency > 1)
	sort.Ints(got)
	for i, v := range got {
		if v != i {
			t.Fatalf("missing item %d, got %v at position %d", i, v, i)
		}
	}
}

func TestMap_Drop(t *testing.T) {
	t.Parallel()

	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	out := Map(1, func(_ context.Context, v int) (int, bool) {
		return v, v > 3 // keep only 4, 5
	})(t.Context(), input)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d: %v", len(got), got)
	}
}

func TestMap_PanicOnZeroConcurrency(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for concurrency < 1")
		}
	}()

	Map(0, func(_ context.Context, v int) (int, bool) { return v, true })
}

func TestFilter(t *testing.T) {
	t.Parallel()

	input := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		input <- i
	}
	close(input)

	out := Filter(func(v int) bool { return v%3 == 0 })(t.Context(), input)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	if len(got) != 3 {
		t.Fatalf("expected [3 6 9], got %v", got)
	}
}

func TestForEach(t *testing.T) {
	t.Parallel()

	input := make(chan int, 3)
	input <- 1
	input <- 2
	input <- 3
	close(input)

	var sum atomic.Int32
	out := ForEach(func(v int) {
		sum.Add(int32(v))
	})(t.Context(), input)

	// ForEach forwards items — collect them
	var got []int
	for v := range out {
		got = append(got, v)
	}

	if sum.Load() != 6 {
		t.Fatalf("expected side effect sum 6, got %d", sum.Load())
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 forwarded items, got %d", len(got))
	}
}

func TestBuffer(t *testing.T) {
	t.Parallel()

	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	out := Buffer[int](10)(t.Context(), input)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	if len(got) != 5 {
		t.Fatalf("expected 5 items, got %d", len(got))
	}
}

func TestBuffer_PanicOnZero(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for buffer size < 1")
		}
	}()

	Buffer[int](0)
}

func TestTee(t *testing.T) {
	t.Parallel()

	input := make(chan int, 3)
	input <- 1
	input <- 2
	input <- 3
	close(input)

	side := make(chan int, 10)
	out := Tee(side)(t.Context(), input)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	close(side)

	var sideGot []int
	for v := range side {
		sideGot = append(sideGot, v)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 forwarded items, got %d", len(got))
	}
	if len(sideGot) != 3 {
		t.Fatalf("expected 3 side items, got %d", len(sideGot))
	}
}

func TestTee_NilSide(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil side channel")
		}
	}()

	Tee[int](nil)
}

func TestTake(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	input := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		input <- i
	}
	close(input)

	out := Take[int](3)(ctx, input)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d: %v", len(got), got)
	}
}

func TestTake_Zero(t *testing.T) {
	t.Parallel()

	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	out := Take[int](0)(t.Context(), input)

	count := 0
	for range out {
		count++
	}

	if count != 0 {
		t.Fatalf("expected 0 items from Take(0), got %d", count)
	}
}

func TestSink(t *testing.T) {
	t.Parallel()

	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	var mu sync.Mutex
	var collected []int

	out := Sink(2, func(_ context.Context, v int) {
		mu.Lock()
		collected = append(collected, v)
		mu.Unlock()
	})(t.Context(), input)

	// Sink forwards nothing — wait for close
	for range out {
		t.Fatal("Sink should not forward items")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(collected) != 5 {
		t.Fatalf("expected 5 items consumed, got %d", len(collected))
	}
}

func TestSink_PanicOnZeroConcurrency(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for concurrency < 1")
		}
	}()

	Sink(0, func(_ context.Context, v int) {})
}
