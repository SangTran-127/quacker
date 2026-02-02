// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

package workerpool

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Task represents a unit of work that can be executed by the worker pool.
// Types implementing this interface must provide an Execute method that performs
// the actual work and a GetID method that returns a unique identifier for the task.
type Task[T any] interface {
	Execute(ctx context.Context) error
	GetID() string
}

// WorkerPool manages a fixed-size pool of workers that process tasks concurrently.
// It provides task queuing, graceful shutdown, error handling, and metrics collection.
// The pool is generic and can process any task type that implements the Task interface.
type WorkerPool[T Task[T]] struct {
	name         string
	numWorkers   int
	taskQueue    chan T
	metrics      *metrics
	errgroup     *errgroup.Group
	once         sync.Once
	ctx          context.Context
	errHandler   func(workerId int, task T, err error)
	panicHandler func(workerId int, task T, panicValue any)
}

// WorkerPoolConfig holds configuration parameters for creating a new WorkerPool.
// Name is used for identifying the pool in metrics and logs.
// NumWorkers specifies the number of concurrent workers.
// TaskSize defines the buffer capacity of the task queue.
// ErrorHandler is an optional callback invoked when a task execution fails.
type WorkerPoolConfig[T any] struct {
	Name       string
	NumWorkers int
	TaskSize   int
	// Handler
	ErrorHandler func(workerId int, task T, err error)
	PanicHandler func(workerID int, task T, panicValue any)
}

// This metrics type internal only for worker_pool
// Do not exposed these type, including mutex
type metrics struct {
	mu             sync.RWMutex
	TasksProcessed map[string]int // [worker_id] -> count
	TasksFailed    int
	ActiveWorkers  int
}

// MetricsSnapshot provides a point-in-time view of worker pool metrics.
// It contains counts of processed tasks per worker, total failed tasks,
// and the number of active workers. This is a safe copy that can be
// inspected without holding locks.
type MetricsSnapshot struct {
	TasksProcessed map[string]int // [worker_id] -> count
	TasksFailed    int
	ActiveWorkers  int
}

// NewWorkerPool creates and initializes a new WorkerPool with the provided configuration.
// It validates that TaskSize and NumWorkers are at least 1, and sets up the task queue,
// error group, and metrics. The provided context is used for coordinating graceful shutdown.
// Returns an error if the configuration is invalid.
func NewWorkerPool[T Task[T]](ctx context.Context, cfg *WorkerPoolConfig[T]) (*WorkerPool[T], error) {
	if cfg.TaskSize < 1 {
		return nil, fmt.Errorf("task queue size must be at least 1, got %d", cfg.TaskSize)
	}

	if cfg.NumWorkers < 1 {
		return nil, fmt.Errorf("number of workers must be at least 1, got %d", cfg.NumWorkers)
	}

	errgroup, ctx := errgroup.WithContext(ctx)
	return &WorkerPool[T]{
		name:       cfg.Name,
		numWorkers: cfg.NumWorkers,
		taskQueue:  make(chan T, cfg.TaskSize),
		ctx:        ctx,
		errgroup:   errgroup,
		metrics: &metrics{
			TasksProcessed: make(map[string]int),
		},
		errHandler:   cfg.ErrorHandler,
		panicHandler: cfg.PanicHandler,
	}, nil
}

// Start launches the configured number of worker goroutines to begin processing tasks.
// Each worker runs independently and will process tasks from the queue until Stop is called
// or the context is cancelled. This method does not block; workers run in the background.
// Returns nil on successful startup.
func (w *WorkerPool[T]) Start() error {

	for i := 0; i < w.numWorkers; i++ {
		workerId := i
		w.errgroup.Go(func() error {

			w.metrics.mu.Lock()
			w.metrics.ActiveWorkers++
			w.metrics.mu.Unlock()

			defer func() {
				w.metrics.mu.Lock()
				w.metrics.ActiveWorkers--
				w.metrics.mu.Unlock()
			}()

			return w.worker(workerId)
		})
	}
	return nil
}

// Push adds a task to the worker pool's queue for processing.
// It returns immediately if the queue has capacity, or returns an error if:
// - The queue is full (non-blocking check)
// - The pool's context has been cancelled
// This method is safe to call concurrently from multiple goroutines.
func (w *WorkerPool[T]) Push(task T) error {
	select {
	case w.taskQueue <- task:
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
		return fmt.Errorf("worker pool %q: task queue is full (capacity: %d)", w.name, cap(w.taskQueue))
	}
}

// Stop gracefully shuts down the worker pool by closing the task queue.
// This signals all workers to finish processing their current tasks and exit.
// Call Wait after Stop to block until all workers have completed.
func (w *WorkerPool[T]) Stop() {
	// Prevent calling close 2 times that will panic
	w.once.Do(func() {
		close(w.taskQueue)
	})
}

// Wait blocks until all worker goroutines have finished processing.
// This should be called after Stop to ensure graceful shutdown.
// Returns an error if any worker encountered an error during execution
// or if the context was cancelled.
func (w *WorkerPool[T]) Wait() error {
	return w.errgroup.Wait()
}

// StopAndWait is a convenience method that combines Stop and Wait.
// It closes the task queue and blocks until all workers have finished
// processing their current tasks. This is equivalent to calling Stop()
// followed by Wait().
//
// Returns an error if any worker encountered an error during execution
// or if the context was cancelled.
func (w *WorkerPool[T]) StopAndWait() error {
	w.Stop()
	return w.Wait()
}

// GetMetrics returns a snapshot of the current pool metrics including task counts
// per worker, total failed tasks, and active worker count. The returned snapshot
// is a safe copy that won't be affected by subsequent metric updates.
// This method is thread-safe and uses a read lock.
func (w *WorkerPool[T]) GetMetrics() MetricsSnapshot {
	w.metrics.mu.RLock()
	defer w.metrics.mu.RUnlock()

	tasksProcessed := make(map[string]int, len(w.metrics.TasksProcessed))
	maps.Copy(tasksProcessed, w.metrics.TasksProcessed)

	return MetricsSnapshot{
		ActiveWorkers:  w.metrics.ActiveWorkers,
		TasksFailed:    w.metrics.TasksFailed,
		TasksProcessed: tasksProcessed,
	}
}

// worker is the main loop for a worker goroutine. It continuously receives tasks
// from the task queue and executes them until either the queue is closed or the
// context is cancelled. Each task is executed with panic recovery to ensure one
// failing task doesn't crash the entire worker.
func (w *WorkerPool[T]) worker(workerId int) error {
	for {
		select {
		case task, ok := <-w.taskQueue:
			if !ok {
				return nil
			}
			w.executeSafe(workerId, task)
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}
}

// executeSafe executes a task with panic recovery and error handling.
// It ensures that panics or errors during task execution don't crash the worker.
// Failures are recorded in metrics, and if configured, the error handler is invoked.
// This implements a resilient pattern where individual task failures don't affect
// other tasks or workers, similar to Promise.allSettled in JavaScript.
func (w *WorkerPool[T]) executeSafe(workerId int, task T) {
	workerName := fmt.Sprintf("%s.worker.%d", w.name, workerId)
	defer func() {
		if r := recover(); r != nil {
			w.recordTaskFailed()
			if w.panicHandler != nil {
				w.panicHandler(workerId, task, r)
			}
		}
	}()

	if err := task.Execute(w.ctx); err != nil {
		w.recordTaskFailed()
		if w.errHandler != nil {
			w.errHandler(workerId, task, err)
		}
		return
	}

	w.recordTaskSuccess(workerName)
}

// recordTaskSuccess increments the task processed count for the specified worker.
// This method is thread-safe and updates metrics under a write lock.
func (w *WorkerPool[T]) recordTaskSuccess(workerName string) {
	w.metrics.mu.Lock()
	defer w.metrics.mu.Unlock()
	w.metrics.TasksProcessed[workerName]++
}

// recordTaskFailed increments the total count of failed tasks.
// This method is thread-safe and updates metrics under a write lock.
func (w *WorkerPool[T]) recordTaskFailed() {
	w.metrics.mu.Lock()
	defer w.metrics.mu.Unlock()
	w.metrics.TasksFailed++
}
