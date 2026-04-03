package pipeline

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestRun_Passthrough(t *testing.T) {
	t.Parallel()

	input := make(chan int, 3)
	input <- 1
	input <- 2
	input <- 3
	close(input)

	// Zero stages — input returned directly
	out := Run(t.Context(), input)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
}

func TestRun_NilInput(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil input")
		}
	}()

	Run[int](t.Context(), nil)
}

func TestRun_Composition(t *testing.T) {
	t.Parallel()

	input := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		input <- i
	}
	close(input)

	// Double even numbers, drop odds
	out := Run(t.Context(), input,
		Filter(func(v int) bool { return v%2 == 0 }),
		Map(1, func(_ context.Context, v int) (int, bool) {
			return v * 2, true
		}),
	)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	// Evens 2,4,6,8,10 doubled = 4,8,12,16,20
	if len(got) != 5 {
		t.Fatalf("expected 5 items, got %d: %v", len(got), got)
	}

	sum := 0
	for _, v := range got {
		sum += v
	}
	if sum != 60 {
		t.Fatalf("expected sum 60, got %d", sum)
	}
}

func TestRun_Backpressure(t *testing.T) {
	t.Parallel()

	input := make(chan int) // unbuffered
	consumed := make(chan int, 10)

	// Map processes the item, observer confirms it was consumed
	out := Run(t.Context(), input,
		Map(1, func(_ context.Context, v int) (int, bool) {
			consumed <- v
			return v, true
		}),
	)

	// Send one item, wait for Map to consume it
	input <- 42
	v := <-consumed
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}

	// Map is now blocked trying to send to out (unbuffered, nobody reading).
	// So it can't read from input again. Producer should block.
	select {
	case input <- 99:
		t.Fatal("producer was not blocked — backpressure broken")
	case <-time.After(50 * time.Millisecond):
		// Backpressure confirmed
	}

	// Clean up
	go func() {
		for range out {
		}
	}()
	close(input)
}

func TestRun_GracefulDrain(t *testing.T) {
	t.Parallel()

	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input) // graceful: close input, pipeline drains

	var count atomic.Int32
	out := Run(t.Context(), input,
		Sink(2, func(_ context.Context, v int) {
			count.Add(1)
		}),
	)

	for range out {
	}

	if count.Load() != 5 {
		t.Fatalf("expected all 5 items drained, got %d", count.Load())
	}
}

func TestRun_Cancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	input := make(chan int)
	out := Run(ctx, input,
		Map(1, func(_ context.Context, v int) (int, bool) {
			return v, true
		}),
	)

	// Send one item so goroutines are active
	go func() {
		select {
		case input <- 1:
		case <-ctx.Done():
		}
	}()

	cancel()

	// Pipeline should shut down — out closes
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
		t.Fatal("pipeline did not shut down after cancellation")
	}
}

func TestRun_ErrorPropagation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	sentinel := fmt.Errorf("bad item")

	out := Run(ctx, input,
		Map(1, func(_ context.Context, v int) (int, bool) {
			if v == 3 {
				cancel(sentinel)
				return v, false
			}
			return v * 2, true
		}),
	)

	for range out {
	}

	if cause := context.Cause(ctx); cause != sentinel {
		t.Fatalf("expected sentinel error, got: %v", cause)
	}
}

func TestObserver(t *testing.T) {
	t.Parallel()

	var items, drops atomic.Int32
	doneCh := make(chan struct{})

	obs := &testObserver{
		onItem: func() { items.Add(1) },
		onDrop: func() { drops.Add(1) },
		onDone: func() { close(doneCh) },
	}

	input := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		input <- i
	}
	close(input)

	// Filter evens (drop odds)
	out := Run(t.Context(), input,
		Filter(func(v int) bool { return v%2 == 0 }, WithObserver(obs)),
	)

	for range out {
	}
	<-doneCh

	if items.Load() != 3 {
		t.Errorf("expected 3 items, got %d", items.Load())
	}
	if drops.Load() != 3 {
		t.Errorf("expected 3 drops, got %d", drops.Load())
	}
}

// testObserver is a simple StageObserver for testing.
type testObserver struct {
	onItem func()
	onDrop func()
	onDone func()
}

func (o *testObserver) OnItem() {
	if o.onItem != nil {
		o.onItem()
	}
}

func (o *testObserver) OnDrop() {
	if o.onDrop != nil {
		o.onDrop()
	}
}

func (o *testObserver) OnDone() {
	if o.onDone != nil {
		o.onDone()
	}
}
