package workerpool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type mockTask struct {
	id string
	fn func(ctx context.Context) error
}

func (t *mockTask) Execute(ctx context.Context) error {
	if t.fn != nil {
		return t.fn(ctx)
	}
	return nil
}

func (t *mockTask) GetID() string {
	return t.id
}

// mockObserver implements WorkerPoolObserver for testing
type mockObserver struct {
	mu             sync.Mutex
	workerStarts   []int
	workerStops    []int
	tasksReceived  []string
	tasksCompleted []string
	tasksFailed    []string
	tasksPanicked  []string
	t              *testing.T
}

func (o *mockObserver) OnWorkerStart(workerID int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.workerStarts = append(o.workerStarts, workerID)
	if o.t != nil {
		o.t.Logf("Observer: worker %d started", workerID)
	}
}

func (o *mockObserver) OnWorkerStop(workerID int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.workerStops = append(o.workerStops, workerID)
	if o.t != nil {
		o.t.Logf("Observer: worker %d stopped", workerID)
	}
}

func (o *mockObserver) OnTaskReceived(workerID int, taskID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tasksReceived = append(o.tasksReceived, taskID)
	if o.t != nil {
		o.t.Logf("Observer: worker %d received task %s", workerID, taskID)
	}
}

func (o *mockObserver) OnTaskCompleted(workerID int, taskID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tasksCompleted = append(o.tasksCompleted, taskID)
	if o.t != nil {
		o.t.Logf("Observer: worker %d completed task %s", workerID, taskID)
	}
}

func (o *mockObserver) OnTaskFailed(workerID int, taskID string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tasksFailed = append(o.tasksFailed, taskID)
	if o.t != nil {
		o.t.Logf("Observer: worker %d task %s failed: %v", workerID, taskID, err)
	}
}

func (o *mockObserver) OnTaskPanic(workerID int, taskID string, panicValue any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tasksPanicked = append(o.tasksPanicked, taskID)
	if o.t != nil {
		o.t.Logf("Observer: worker %d task %s panicked: %v", workerID, taskID, panicValue)
	}
}

func TestNewWorkerPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		opts      []Option
		shouldErr bool
	}{
		{
			name:      "valid config with defaults",
			opts:      []Option{},
			shouldErr: false,
		},
		{
			name: "valid config with all options",
			opts: []Option{
				WithName("test-pool"),
				WithNumWorkers(4),
				WithTaskQueueSize(100),
			},
			shouldErr: false,
		},
		{
			name: "invalid number of workers (zero)",
			opts: []Option{
				WithNumWorkers(0),
			},
			shouldErr: true,
		},
		{
			name: "invalid number of workers (negative)",
			opts: []Option{
				WithNumWorkers(-1),
			},
			shouldErr: true,
		},
		{
			name: "invalid task queue size (zero)",
			opts: []Option{
				WithTaskQueueSize(0),
			},
			shouldErr: true,
		},
		{
			name: "invalid task queue size (negative)",
			opts: []Option{
				WithTaskQueueSize(-1),
			},
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewWorkerPool[*mockTask](t.Context(), test.opts...)
			if (err != nil) != test.shouldErr {
				t.Errorf("NewWorkerPool() error = %v, shouldErr %v", err, test.shouldErr)
			}
		})
	}
}

func TestWorkerPool_Observer(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	obs := &mockObserver{t: t}
	numWorkers := 2
	taskCount := 5

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("observer-test"),
		WithNumWorkers(numWorkers),
		WithTaskQueueSize(10),
		WithObserver(obs),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()

	// Push successful tasks
	for i := 0; i < taskCount; i++ {
		task := &mockTask{id: fmt.Sprintf("task-%d", i)}
		if err := pool.Push(task); err != nil {
			t.Errorf("failed to push task: %v", err)
		}
	}

	// Give time for tasks to process
	time.Sleep(50 * time.Millisecond)

	pool.StopAndWait()

	obs.mu.Lock()
	defer obs.mu.Unlock()

	// Verify worker lifecycle
	if len(obs.workerStarts) != numWorkers {
		t.Errorf("expected %d worker starts, got %d", numWorkers, len(obs.workerStarts))
	}
	if len(obs.workerStops) != numWorkers {
		t.Errorf("expected %d worker stops, got %d", numWorkers, len(obs.workerStops))
	}

	// Verify task lifecycle
	if len(obs.tasksReceived) != taskCount {
		t.Errorf("expected %d tasks received, got %d", taskCount, len(obs.tasksReceived))
	}
	if len(obs.tasksCompleted) != taskCount {
		t.Errorf("expected %d tasks completed, got %d", taskCount, len(obs.tasksCompleted))
	}
}

func TestWorkerPool_ObserverTaskFailed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	obs := &mockObserver{t: t}

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("observer-fail-test"),
		WithNumWorkers(1),
		WithTaskQueueSize(10),
		WithObserver(obs),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()

	// Push a failing task
	task := &mockTask{
		id: "fail-task",
		fn: func(ctx context.Context) error {
			return fmt.Errorf("intentional error")
		},
	}
	pool.Push(task)

	time.Sleep(50 * time.Millisecond)
	pool.StopAndWait()

	obs.mu.Lock()
	defer obs.mu.Unlock()

	if len(obs.tasksFailed) != 1 {
		t.Errorf("expected 1 failed task, got %d", len(obs.tasksFailed))
	}
	if len(obs.tasksFailed) > 0 && obs.tasksFailed[0] != "fail-task" {
		t.Errorf("expected failed task id 'fail-task', got %s", obs.tasksFailed[0])
	}
}

func TestWorkerPool_ObserverTaskPanic(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	obs := &mockObserver{t: t}

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("observer-panic-test"),
		WithNumWorkers(1),
		WithTaskQueueSize(10),
		WithObserver(obs),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()

	// Push a panicking task
	task := &mockTask{
		id: "panic-task",
		fn: func(ctx context.Context) error {
			panic("intentional panic")
		},
	}
	pool.Push(task)

	time.Sleep(50 * time.Millisecond)
	pool.StopAndWait()

	obs.mu.Lock()
	defer obs.mu.Unlock()

	if len(obs.tasksPanicked) != 1 {
		t.Errorf("expected 1 panicked task, got %d", len(obs.tasksPanicked))
	}
	if len(obs.tasksPanicked) > 0 && obs.tasksPanicked[0] != "panic-task" {
		t.Errorf("expected panicked task id 'panic-task', got %s", obs.tasksPanicked[0])
	}
}

func TestWorkerPool_ConcurrentPush(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var wg sync.WaitGroup
	taskCount := 20

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("concurrent-push"),
		WithNumWorkers(3),
		WithTaskQueueSize(taskCount),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()
	defer pool.StopAndWait()

	for i := range taskCount {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task := &mockTask{id: fmt.Sprintf("task-%d", id)}
			if err := pool.Push(task); err != nil {
				t.Errorf("push failed: %v", err)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout - possible deadlock")
	}
}

func TestWorkerPool_TaskQueueFull(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("queue-full"),
		WithNumWorkers(1),
		WithTaskQueueSize(2),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	// Don't start the pool - queue will fill up
	for i := 0; i < 2; i++ {
		task := &mockTask{id: fmt.Sprintf("task-%d", i)}
		if err := pool.Push(task); err != nil {
			t.Errorf("unexpected error on push %d: %v", i, err)
		}
	}

	// Third push should fail
	task := &mockTask{id: "overflow-task"}
	err = pool.Push(task)
	if err == nil {
		t.Error("expected error when queue is full, got nil")
	}
}

func TestWorkerPool_MultipleStop(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("multiple-stop"),
		WithNumWorkers(2),
		WithTaskQueueSize(10),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()

	// First stop
	pool.Stop()

	// Second stop should not panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Stop() panicked on second call: %v", r)
			}
		}()
		pool.Stop()
	}()

	pool.Wait()
}

func TestWorkerPool_MetricsThreadSafe(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var wg sync.WaitGroup
	taskCount := 20

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("metrics-safe"),
		WithNumWorkers(10),
		WithTaskQueueSize(taskCount),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()
	defer pool.StopAndWait()

	// Concurrent task pushes
	for i := range taskCount {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task := &mockTask{
				id: fmt.Sprintf("task-%d", id),
				fn: func(ctx context.Context) error {
					time.Sleep(time.Millisecond)
					return nil
				},
			}
			pool.Push(task)
		}(i)
	}

	// Concurrent metric reads
	concurrentReads := 100
	for range concurrentReads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				pool.GetMetrics()
			}
		}()
	}

	wg.Wait()
}

func TestWorkerPool_ContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("context-cancel"),
		WithNumWorkers(2),
		WithTaskQueueSize(10),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()

	// Push some tasks
	for i := 0; i < 5; i++ {
		task := &mockTask{id: fmt.Sprintf("task-%d", i)}
		pool.Push(task)
	}

	// Cancel the context
	cancel()

	// Wait should return (possibly with context error)
	err = pool.Wait()
	if err != nil && err != context.Canceled {
		t.Logf("Wait returned: %v", err)
	}
}

// TestWorkerPool_ErrorHandler tests the ErrorHandler option for error handling logic.
func TestWorkerPool_ErrorHandler(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	errorHandlerCalled := make(chan struct{})

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("error-handler-test"),
		WithNumWorkers(1),
		WithTaskQueueSize(10),
		WithErrorHandler(func(workerID int, taskID string, err error) {
			t.Logf("ErrorHandler called: worker=%d, task=%s, err=%v", workerID, taskID, err)
			close(errorHandlerCalled)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()

	task := &mockTask{
		id: "error-task",
		fn: func(ctx context.Context) error {
			return fmt.Errorf("task error")
		},
	}
	pool.Push(task)

	select {
	case <-errorHandlerCalled:
		t.Log("ErrorHandler was called successfully")
	case <-time.After(time.Second):
		t.Error("ErrorHandler was not called")
	}

	pool.StopAndWait()
}

// TestWorkerPool_PanicHandler tests the PanicHandler option for panic handling logic.
func TestWorkerPool_PanicHandler(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	panicHandlerCalled := make(chan struct{})

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("panic-handler-test"),
		WithNumWorkers(1),
		WithTaskQueueSize(10),
		WithPanicHandler(func(workerID int, taskID string, panicValue any) {
			t.Logf("PanicHandler called: worker=%d, task=%s, panic=%v", workerID, taskID, panicValue)
			close(panicHandlerCalled)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()

	task := &mockTask{
		id: "panic-task",
		fn: func(ctx context.Context) error {
			panic("task panic")
		},
	}
	pool.Push(task)

	select {
	case <-panicHandlerCalled:
		t.Log("PanicHandler was called successfully")
	case <-time.After(time.Second):
		t.Error("PanicHandler was not called")
	}

	pool.StopAndWait()
}

// TestWorkerPool_PushContextCancelled tests Push() when context is already cancelled.
func TestWorkerPool_PushContextCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("push-ctx-cancelled"),
		WithNumWorkers(1),
		WithTaskQueueSize(1),
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	// Cancel context before pushing
	cancel()

	// Fill the queue first to force the select to check ctx.Done()
	task1 := &mockTask{id: "task-1"}
	pool.Push(task1) // This might succeed if queue has space

	// Now try to push when queue is full and context is cancelled
	task2 := &mockTask{id: "task-2"}
	err = pool.Push(task2)

	// Should get either queue full error or context cancelled error
	if err == nil {
		t.Log("Push succeeded (queue had space)")
	} else if err == context.Canceled {
		t.Log("Push returned context.Canceled as expected")
	} else {
		t.Logf("Push returned: %v", err)
	}
}

// TestWorkerPool_NoObserver tests execution without observer (nil observer paths).
func TestWorkerPool_NoObserver(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	pool, err := NewWorkerPool[*mockTask](ctx,
		WithName("no-observer"),
		WithNumWorkers(2),
		WithTaskQueueSize(10),
		// No observer set
	)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	pool.Start()

	// Push successful task
	task1 := &mockTask{id: "success-task"}
	pool.Push(task1)

	// Push failing task
	task2 := &mockTask{
		id: "fail-task",
		fn: func(ctx context.Context) error {
			return fmt.Errorf("error")
		},
	}
	pool.Push(task2)

	// Push panicking task
	task3 := &mockTask{
		id: "panic-task",
		fn: func(ctx context.Context) error {
			panic("panic")
		},
	}
	pool.Push(task3)

	time.Sleep(50 * time.Millisecond)
	pool.StopAndWait()

	// Verify metrics
	metrics := pool.GetMetrics()
	if metrics.TasksFailed != 2 {
		t.Errorf("expected 2 failed tasks, got %d", metrics.TasksFailed)
	}
}
