package workerpool

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

type mockTask struct {
	id      string
	Message string
	// This fn callback will help you custom execute for each test
	fn func(ctx context.Context) error
}

func (c *mockTask) Execute(ctx context.Context) error {

	if c.fn != nil {
		// forward call
		return c.fn(ctx)
	}
	return nil
}

func (c *mockTask) GetID() string {
	return c.id
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewWorkerPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    WorkerPoolConfig[*mockTask]
		shouldErr bool
	}{
		{
			name: "config with valid field",
			config: WorkerPoolConfig[*mockTask]{
				Name:       "test-config-validate-1",
				NumWorkers: 1,
				TaskSize:   1,
			},
			shouldErr: false,
		},
		{
			name: "config with invalid number of workers",
			config: WorkerPoolConfig[*mockTask]{
				Name:       "test-config-validate-2",
				NumWorkers: 0,
				TaskSize:   1,
			},
			shouldErr: true,
		},
		{
			name: "config with invalid task sizes",
			config: WorkerPoolConfig[*mockTask]{
				Name:       "test-config-validate-3",
				NumWorkers: 1,
				TaskSize:   0,
			},
			shouldErr: true,
		},
		{
			name: "config with invalid both worker number and task size",
			config: WorkerPoolConfig[*mockTask]{
				Name:       "test-config-validate-4",
				NumWorkers: 0,
				TaskSize:   0,
			},
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewWorkerPool[*mockTask](t.Context(), &test.config)
			if (err != nil) != test.shouldErr {
				log.Fatalf("Validation error = %v, want err %v", err, test.shouldErr)
			}
		})
	}
}

func TestWorkerPool_ConcurrentPush(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var wg sync.WaitGroup
	taskSize := 20

	pool, err := NewWorkerPool(ctx, &WorkerPoolConfig[*mockTask]{
		Name:       "test_concurrent_push",
		NumWorkers: 3,
		TaskSize:   taskSize,
	})

	if err != nil {
		t.Errorf("cannot initialize new worker pool %v", err)
	}

	if err := pool.Start(); err != nil {
		t.Errorf("cannot start pool %s, error: %v", pool.name, err)
	}

	defer pool.StopAndWait()
	// Spawn 20 goroutine simulate to push 20 item
	wg.Add(taskSize)
	for i := range taskSize {
		go func(id int) {
			defer wg.Done()
			task := &mockTask{
				id:      fmt.Sprintf("%d", id),
				Message: fmt.Sprintf("Message: %d", id),
			}

			if err := pool.Push(task); err != nil {
				t.Errorf("push failed %s", err.Error())
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
		return
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout - possible deadlock")
	}

	pool.StopAndWait()
}

func TestWorkerPool_ErrorHandling(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	task := &mockTask{
		fn: func(ctx context.Context) error {
			return fmt.Errorf("something went wrong from mockTask")
		},
	}

	signal := make(chan struct{})

	pool, err := NewWorkerPool(ctx, &WorkerPoolConfig[*mockTask]{
		Name:       "error_handling",
		NumWorkers: 1,
		TaskSize:   1,
		ErrorHandler: func(workerId int, task *mockTask, err error) {
			t.Logf("err received: %s from worker %d", err.Error(), workerId)
			signal <- struct{}{}
		},
	})

	if err != nil {
		t.Errorf("cannot initialize new worker pool %v", err)
	}

	if err := pool.Start(); err != nil {
		t.Errorf("cannot start pool %s, error: %v", pool.name, err)
	}

	defer pool.StopAndWait()

	if err := pool.Push(task); err != nil {
		t.Errorf("cannot push task to queue")
	}

	select {
	case <-signal:
		// compare with 1 because I just launch single failed task
		if pool.metrics.TasksFailed != 1 {
			t.Errorf("task failed count mismatch expect 1 but received %d", pool.metrics.TasksFailed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout - possible deadlock")

	}
}

func TestWorkerPool_PanicHandling(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	task := &mockTask{
		fn: func(ctx context.Context) error {
			panic("omg! im so panic!!!")
		},
	}

	done := make(chan struct{})

	pool, err := NewWorkerPool(ctx, &WorkerPoolConfig[*mockTask]{
		Name:       "panic_handling",
		TaskSize:   1,
		NumWorkers: 1,
		PanicHandler: func(workerID int, task *mockTask, panicValue any) {
			t.Logf("panic throw at worker: %d, panic value: %v", workerID, panicValue)
			done <- struct{}{}
		},
	})

	if err != nil {
		t.Errorf("cannot initialize new worker pool %v", err)
	}

	if err := pool.Start(); err != nil {
		t.Errorf("cannot start pool %s, error: %v", pool.name, err)
	}

	defer pool.StopAndWait()

	if err := pool.Push(task); err != nil {
		t.Errorf("cannot push task %s to queue, error: %v", task.id, err)
	}

	select {
	case <-done:
		// panic also mark as failed
		if pool.metrics.TasksFailed != 1 {
			t.Error("panic task failed count mismatch")
		}
		t.Log("pannic handle success")
	case <-time.After(5 * time.Second):
		t.Error("panic handler test timeout error")
	}
}

func TestWorkerPool_MetricsThreadSafe(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var wg sync.WaitGroup
	taskSize := 20

	pool, err := NewWorkerPool(ctx, &WorkerPoolConfig[*mockTask]{
		Name:       "metrics_thread_safe",
		NumWorkers: 10,
		TaskSize:   taskSize,
	})

	if err != nil {
		t.Errorf("cannot initialize new worker pool %v", err)
	}

	if err := pool.Start(); err != nil {
		t.Errorf("cannot start pool %s, error: %v", pool.name, err)
	}

	defer pool.StopAndWait()

	// simulate concurrency push task
	wg.Add(taskSize)
	for i := range taskSize {
		go func(id int) {
			defer wg.Done()
			// create new task
			task := &mockTask{
				id: fmt.Sprint(i),
				fn: func(ctx context.Context) error {
					time.Sleep(1 * time.Millisecond)
					return nil
				},
			}
			pool.Push(task)
		}(i)
	}

	councurrencyRead := 100
	wg.Add(councurrencyRead)
	for range councurrencyRead {
		go func() {
			defer wg.Done()
			for range 10 {
				pool.GetMetrics()
			}
		}()
	}

	wg.Wait()
}

func TestWorkerPool_TaskQueueFull(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	taskOverflowSize := 100

	pool, err := NewWorkerPool(ctx, &WorkerPoolConfig[*mockTask]{
		Name:       "task_queue_full",
		NumWorkers: 10,
		TaskSize:   10,
	})

	if err != nil {
		t.Errorf("cannot initialize new worker pool %v", err)
	}

	for i := range taskOverflowSize {
		// create new task
		task := &mockTask{
			id: fmt.Sprint(i),
			fn: func(ctx context.Context) error {
				time.Sleep(1 * time.Millisecond)
				return nil
			},
		}
		if err := pool.Push(task); err != nil {
			// overflow queue full should goes here
			t.Log(err)
			// cancel the worker pool
			cancel()
		}
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Error("task queue full test timeout error")
	}
}

func TestWorkerPool_TaskCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// push these task first
	taskSize := 10

	pool, err := NewWorkerPool(ctx, &WorkerPoolConfig[*mockTask]{
		Name:       "task_cancellation",
		NumWorkers: 2,
		TaskSize:   taskSize,
	})

	if err != nil {
		t.Errorf("cannot initialize new worker pool %v", err)
	}

	for range taskSize {
		// no need concurrency
		task := &mockTask{}
		if err := pool.Push(task); err != nil {
			t.Errorf("cannot push task %s to queue, error: %v", task.id, err)
		}
	}

	pool.Start()

	pool.Stop()

	// cancel here to trigger
	cancel()

	pool.Wait()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Error("task canncellation test timeout error")
	}
}
