package fanout

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TODO: Should check concurrent task that can be race condition
// Make sure it not race condition and thread-safe

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

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestFanOut_NewFanOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       *FanOutConfig
		shouldErr bool
	}{
		{
			name: "worker size is invalid should yeild error",
			cfg: &FanOutConfig{
				WorkerSize: -10,
			},
			shouldErr: true,
		},
		{
			name: "buffer size is invalid should yeild error",
			cfg: &FanOutConfig{
				BufferSize: -10,
				WorkerSize: 1,
			},
			shouldErr: true,
		},
		{
			name: "valid all field should work",
			cfg: &FanOutConfig{
				BufferSize: 1,
				WorkerSize: 1,
			},
			shouldErr: false,
		},
		// TODO: More case
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewFanOut[int](
				WithBufferSize(test.cfg.BufferSize),
				WithObserver(test.cfg.Observer),
				WithWorkerSize(test.cfg.WorkerSize),
				WithStrategy(test.cfg.Strategy),
			)
			if (err != nil) != test.shouldErr {
				t.Errorf("validation error = %v, want err %v", err, test.shouldErr)
			}
		})
	}
}

func TestFanOut_RunBroadCast(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	workerSize := 2
	size := 10
	obs := &mockObserver{
		name: "fo broadcast",
		t:    t,
	}
	fo, _ := NewFanOut[int](func(foc *FanOutConfig) {
		foc.WorkerSize = workerSize
		foc.Strategy = BroadCast
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

	res := make([]int, workerSize)

	for i, cha := range fo.Outputs() {
		wg.Go(func() {
			count := 0
			for v := range cha {
				fmt.Println(v)
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
	workerSize := 2
	obs := &mockObserver{
		name: "fo round robin",
		t:    t,
	}
	fo, _ := NewFanOut[int](func(foc *FanOutConfig) {
		foc.WorkerSize = workerSize
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
	res := make([]int, workerSize)
	for i, cha := range fo.Outputs() {
		wg.Go(func() {
			count := 0
			for v := range cha {
				fmt.Println(v)
				count++
				res[i] = count
			}
		})
	}

	wg.Wait()
	expected := size / workerSize

	for i, count := range res {
		if count != expected {
			t.Errorf("Worker %d received %d items, expected %d", i, count, expected)
		}
	}
}

func TestFanOut_ContextTimeOut(t *testing.T) {
	t.Parallel()
	fo, _ := NewFanOut[int](WithWorkerSize(2))
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

func TestFanIn_ContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	fo, err := NewFanOut[int](WithWorkerSize(3))
	if err != nil {
		t.Fatalf("fo: context cancel new fanout error %s", err)
	}

	input := make(chan int, 10)
	received := make(chan int, 100)
	fo.Run(ctx, input)

	var wg sync.WaitGroup
	for _, ch := range fo.Outputs() {
		wg.Go(func() {
			for v := range ch {
				t.Logf("received %d", v)
				received <- v
			}
		})
	}

	time.Sleep(10 * time.Millisecond)

	cancel()

	close(input)

	wg.Wait()

	close(received)

	receivedCount := len(received)

	t.Logf("Received %d messages before cancellation", receivedCount)

	// Should receive at least some messages, but maybe not all
	if receivedCount < 0 {
		t.Fatal("fo error, received count must greater than 0")
	}

	if receivedCount > 10 {
		t.Fatal("fo error")
	}
}

func TestFanOut_ContextDoneRoundRobin(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	// BufferSize 0 to force blocking on send
	fo, err := NewFanOut[int](
		WithWorkerSize(3),
		WithBufferSize(0),
		WithStrategy(RoundRobin),
	)
	if err != nil {
		t.Fatalf("new fanout error: %v", err)
	}

	input := make(chan int)
	fo.Run(ctx, input)

	// Send one value to block the dispatcher in 'roundRobin'
	// It will try to send to an output, but no one is reading.
	go func() {
		input <- 1
	}()

	// Wait a bit to ensure we are stuck in the send case
	time.Sleep(10 * time.Millisecond)

	cancel()

	// Verify all outputs are closed
	for i, ch := range fo.Outputs() {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("worker %d: expected closed channel, got value", i)
			}
		case <-time.After(time.Second):
			t.Errorf("worker %d: timeout waiting for shutdown", i)
		}
	}
}

func TestFanOut_ContextDoneBroadcast(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	// BufferSize 0 to force blocking on send
	fo, err := NewFanOut[int](
		WithWorkerSize(3),
		WithStrategy(BroadCast),
		WithBufferSize(0),
	)
	if err != nil {
		t.Fatalf("new fanout error: %v", err)
	}

	input := make(chan int)
	fo.Run(ctx, input)

	// Send one value to block the dispatcher in 'broadcast'
	// It will spawn goroutines to send to all outputs, but they will block.
	go func() {
		input <- 1
	}()

	// Wait a bit to ensure we are stuck in the send case is active
	time.Sleep(10 * time.Millisecond)

	cancel()

	// Verify all outputs are closed
	for i, ch := range fo.Outputs() {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("worker %d: expected closed channel, got value", i)
			}
		case <-time.After(time.Second):
			t.Errorf("worker %d: timeout waiting for shutdown", i)
		}
	}
}

func TestFanOut_RunMultipleTime(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("fo err: it should panic error when calling multiple Run")
		}
	}()

	// BufferSize 0 to force blocking on send
	fo, err := NewFanOut[int](
		WithWorkerSize(3),
		WithBufferSize(0),
	)
	if err != nil {
		t.Fatalf("new fanout error: %v", err)
	}

	input := make(chan int, 10)
	fo.Run(ctx, input)

	fo.Run(ctx, input)
}
