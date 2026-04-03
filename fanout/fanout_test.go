package fanout

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mockObserver struct {
	name string
	t    *testing.T
}

func (m *mockObserver) OnDistribute() {
	m.t.Logf("fanout: %s OnDistribute", m.name)
}

func (m *mockObserver) OnOutputClosed() {
	m.t.Logf("fanout: %s OnOutputClosed", m.name)
}

func TestFanOut_NewFanOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		opts      []Option
		shouldErr bool
	}{
		{
			name:      "worker size is invalid should yield error",
			opts:      []Option{WithWorkerCount(-10)},
			shouldErr: true,
		},
		{
			name:      "buffer size is invalid should yield error",
			opts:      []Option{WithBufferSize(-10), WithWorkerCount(1)},
			shouldErr: true,
		},
		{
			name:      "valid all field should work",
			opts:      []Option{WithBufferSize(1), WithWorkerCount(1)},
			shouldErr: false,
		},
		{
			name:      "worker count is zero should yield error",
			opts:      []Option{WithWorkerCount(0)},
			shouldErr: true,
		},
		{
			name:      "valid broadcast strategy should work",
			opts:      []Option{WithWorkerCount(5), WithBufferSize(10), WithStrategy(Broadcast)},
			shouldErr: false,
		},
		{
			name:      "invalid strategy should yield error",
			opts:      []Option{WithWorkerCount(1), WithStrategy(FanOutStrategy(99))},
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewFanOut[int](test.opts...)
			if (err != nil) != test.shouldErr {
				t.Errorf("validation error = %v, want err %v", err, test.shouldErr)
			}
		})
	}
}

func TestFanOut_ConcurrentStress(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	workerCount := 5
	fo, err := NewFanOut[int](
		WithWorkerCount(workerCount),
		WithBufferSize(100),
	)
	if err != nil {
		t.Fatalf("new fanout error: %v", err)
	}

	input := make(chan int, 1000)
	fo.Run(ctx, input)

	var wg sync.WaitGroup

	// 1. Concurrent access to Outputs()
	// Outputs() uses sync.Once so it should be safe
	for i := 0; i < 10; i++ {
		wg.Go(func() {
			outs := fo.Outputs()
			if len(outs) != workerCount {
				t.Errorf("expected %d outputs, got %d", workerCount, len(outs))
			}
		})
	}

	// 2. Concurrent consumers
	// Just consume data to prevent blocking
	for _, ch := range fo.Outputs() {
		wg.Go(func() {
			for range ch {
				// consume
			}
		})
	}

	// 3. Concurrent producers
	producers := 10
	messagesPerProducer := 100

	var producerWg sync.WaitGroup
	producerWg.Add(producers)

	for i := 0; i < producers; i++ {
		go func() {
			defer producerWg.Done()
			for j := 0; j < messagesPerProducer; j++ {
				select {
				case input <- j:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Closer goroutine
	go func() {
		producerWg.Wait()
		close(input)
	}()

	// Wait for all consumers and checkers
	wg.Wait()
}

func TestFanOut_RunBroadCast(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	workerCount := 2
	size := 10
	obs := &mockObserver{
		name: "fo broadcast",
		t:    t,
	}
	fo, _ := NewFanOut[int](func(foc *FanOutConfig) {
		foc.WorkerCount = workerCount
		foc.Strategy = Broadcast
		foc.Observer = obs
	})

	ch := make(chan int, size)

	wg.Go(func() {
		for i := range size {
			ch <- i
		}
		close(ch)
	})

	fo.Run(t.Context(), ch)

	res := make([]int, workerCount)

	for i, cha := range fo.Outputs() {
		wg.Go(func() {
			count := 0
			for v := range cha {
				t.Logf("%v", v)
				count++
				res[i] = count
			}
		})
	}

	wg.Wait()

	expected := size

	for i, count := range res {
		if count != expected {
			t.Errorf("Worker %d received %d items, expected %d", i, count, expected)
		}
	}
}

func TestFanOut_RunRoundRobin(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	size := 10
	workerCount := 2
	obs := &mockObserver{
		name: "fo round robin",
		t:    t,
	}
	fo, _ := NewFanOut[int](func(foc *FanOutConfig) {
		foc.WorkerCount = workerCount
		foc.Strategy = RoundRobin
		foc.Observer = obs
	})

	ch := make(chan int, size)
	wg.Go(func() {
		for i := range size {
			ch <- i
		}
		close(ch)
	})

	fo.Run(t.Context(), ch)
	res := make([]int, workerCount)
	for i, cha := range fo.Outputs() {
		wg.Go(func() {
			count := 0
			for v := range cha {
				t.Logf("%v", v)
				count++
				res[i] = count
			}
		})
	}

	wg.Wait()
	expected := size / workerCount

	for i, count := range res {
		if count != expected {
			t.Errorf("Worker %d received %d items, expected %d", i, count, expected)
		}
	}
}

func TestFanOut_ContextTimeOut(t *testing.T) {
	t.Parallel()
	fo, _ := NewFanOut[int](WithWorkerCount(2))
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)

	defer cancel()

	input := make(chan int, 10)

	fo.Run(ctx, input)

	var wg sync.WaitGroup
	done := make(chan struct{})
	for _, ch := range fo.Outputs() {
		wg.Go(func() {
			for v := range ch {
				t.Logf("received %d", v)
			}
		})
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	// Simulate function keep sending data to input
	go func() {
		i := 0
		for {
			select {
			case input <- i:
				i++
				time.Sleep(time.Millisecond)
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-done:
		t.Log("FanOut cleaned up successfully")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("FanOut did not clean up after context timeout")
	}
}

func TestFanOut_ContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	fo, err := NewFanOut[int](WithWorkerCount(3))
	if err != nil {
		t.Fatalf("failed to create fanout: %v", err)
	}

	input := make(chan int, 10)
	fo.Run(ctx, input)

	var wg sync.WaitGroup
	for _, ch := range fo.Outputs() {
		wg.Go(func() {
			for range ch {
				// drain
			}
		})
	}

	cancel()
	close(input)

	// Must complete without hanging — proves cancellation works
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("FanOut cleaned up after cancellation")
	case <-time.After(time.Second):
		t.Fatal("FanOut did not shut down after context cancellation")
	}
}

func TestFanOut_ContextDoneRoundRobin(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	// BufferSize 0 to force blocking on send
	fo, err := NewFanOut[int](
		WithWorkerCount(3),
		WithBufferSize(0),
		WithStrategy(RoundRobin),
	)
	if err != nil {
		t.Fatalf("new fanout error: %v", err)
	}

	// Use buffered input so the dispatcher reads immediately,
	// then blocks on the unbuffered output send.
	input := make(chan int, 1)
	input <- 1
	fo.Run(ctx, input)

	cancel()

	// Verify all outputs close after cancellation
	for i, ch := range fo.Outputs() {
		select {
		case _, ok := <-ch:
			if ok {
				// Value delivered before cancel — drain remaining
				for range ch {
				}
			}
		case <-time.After(time.Second):
			t.Fatalf("worker %d: timeout waiting for channel close after cancellation", i)
		}
	}
}

func TestFanOut_ContextDoneBroadcast(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	// BufferSize 0 to force blocking on send
	fo, err := NewFanOut[int](
		WithWorkerCount(3),
		WithStrategy(Broadcast),
		WithBufferSize(0),
	)
	if err != nil {
		t.Fatalf("new fanout error: %v", err)
	}

	// Use buffered input so the dispatcher reads immediately,
	// then blocks on the unbuffered output sends in broadcast.
	input := make(chan int, 1)
	input <- 1
	fo.Run(ctx, input)

	cancel()

	// Verify all outputs close after cancellation
	for i, ch := range fo.Outputs() {
		select {
		case _, ok := <-ch:
			if ok {
				for range ch {
				}
			}
		case <-time.After(time.Second):
			t.Fatalf("worker %d: timeout waiting for channel close after cancellation", i)
		}
	}
}

func TestFanOut_RunMultipleTime(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	fo, err := NewFanOut[int](
		WithWorkerCount(3),
		WithBufferSize(0),
	)
	if err != nil {
		t.Fatalf("new fanout error: %v", err)
	}

	input := make(chan int, 10)
	fo.Run(ctx, input)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when calling Run() multiple times")
		}
	}()

	fo.Run(ctx, input)
}

func TestFanOut_NilInput(t *testing.T) {
	t.Parallel()

	fo, err := NewFanOut[int](WithWorkerCount(1))
	if err != nil {
		t.Fatalf("failed to create fanout: %v", err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when passing nil input channel")
		}
	}()

	fo.Run(t.Context(), nil)
}
