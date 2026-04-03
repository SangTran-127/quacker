package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestBatch_Full(t *testing.T) {
	t.Parallel()

	input := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		input <- i
	}
	close(input)

	out := Batch(t.Context(), input, 3, time.Second)

	var batches [][]int
	for b := range out {
		batches = append(batches, b)
	}

	// 10 items, batch size 3 → 3 full batches + 1 partial (1 item)
	if len(batches) != 4 {
		t.Fatalf("expected 4 batches, got %d: %v", len(batches), batches)
	}
	if len(batches[0]) != 3 {
		t.Fatalf("expected first batch size 3, got %d", len(batches[0]))
	}
	if len(batches[3]) != 1 {
		t.Fatalf("expected last batch size 1, got %d", len(batches[3]))
	}
}

func TestBatch_TimeoutFlush(t *testing.T) {
	t.Parallel()

	input := make(chan int)

	out := Batch(t.Context(), input, 100, 50*time.Millisecond)

	// Send 2 items (well below batch size of 100)
	input <- 1
	input <- 2

	// Ticker should flush the partial batch
	select {
	case batch := <-out:
		if len(batch) != 2 {
			t.Fatalf("expected batch of 2 from timeout flush, got %d", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for batch flush")
	}

	close(input)
	for range out {
	}
}

func TestBatch_ContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	input := make(chan int)
	out := Batch(ctx, input, 100, time.Second)

	cancel()

	// Output should close after cancellation
	done := make(chan struct{})
	go func() {
		for range out {
		}
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown
	case <-time.After(time.Second):
		t.Fatal("Batch did not shut down after context cancellation")
	}
}

func TestBatch_NilInput(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil input")
		}
	}()

	Batch[int](t.Context(), nil, 10, time.Second)
}

func TestBatch_PanicOnZeroSize(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for batch size < 1")
		}
	}()

	input := make(chan int)
	Batch(t.Context(), input, 0, time.Second)
}
