// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

// Package workerpool provides a generic worker pool implementation for concurrent task processing.
// It manages a fixed-size pool of workers that process tasks from a shared queue, providing
// graceful shutdown, error handling, panic recovery, and metrics collection.
//
// The worker pool pattern is useful when you need to limit concurrency while processing
// a stream of tasks, such as rate-limiting API calls, managing database connections,
// or controlling resource usage.
//
// # Task Interface
//
// Tasks must implement the Task interface:
//
//	type Task[T any] interface {
//	    Execute(ctx context.Context) error
//	    GetID() string
//	}
//
// # Example Usage
//
// Basic worker pool:
//
//	pool, _ := workerpool.NewWorkerPool[MyTask](ctx,
//	    workerpool.WithNumWorkers(4),
//	    workerpool.WithTaskQueueSize(100),
//	)
//	pool.Start()
//	for _, task := range tasks {
//	    pool.Push(task)
//	}
//	pool.StopAndWait()
//
// With observer for monitoring:
//
//	pool, _ := workerpool.NewWorkerPool[MyTask](ctx,
//	    workerpool.WithName("api-processor"),
//	    workerpool.WithNumWorkers(4),
//	    workerpool.WithTaskQueueSize(100),
//	    workerpool.WithObserver(myObserver),
//	)
//	pool.Start()
//	// ... push tasks ...
//	pool.StopAndWait()
//	metrics := pool.GetMetrics()
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

// WorkerPoolObserver defines callbacks for monitoring worker pool lifecycle events.
// Implementations can use these hooks for logging, metrics collection,
// or coordinating with external systems.
//
// All observer methods are called synchronously from the worker goroutines.
// Implementations should be lightweight to avoid blocking task processing.
//
// The lifecycle of events is:
//
//	OnWorkerStart -> [OnTaskReceived -> (OnTaskCompleted | OnTaskFailed | OnTaskPanic)]* -> OnWorkerStop
type WorkerPoolObserver interface {
	// OnWorkerStart is called when a worker goroutine starts processing tasks.
	// This is called once per worker when Start() is invoked.
	OnWorkerStart(workerID int)
	// OnWorkerStop is called when a worker goroutine stops.
	// This is called once per worker during graceful shutdown.
	OnWorkerStop(workerID int)
	// OnTaskReceived is called when a worker receives a task from the queue.
	// This is called before Execute() is invoked on the task.
	OnTaskReceived(workerID int, taskID string)
	// OnTaskCompleted is called when a task completes successfully (Execute returns nil).
	OnTaskCompleted(workerID int, taskID string)
	// OnTaskFailed is called when a task fails with an error (Execute returns non-nil error).
	OnTaskFailed(workerID int, taskID string, err error)
	// OnTaskPanic is called when a task panics during execution.
	// The panic is recovered and logged, but the worker continues processing.
	OnTaskPanic(workerID int, taskID string, panicValue any)
}

// WorkerPoolConfig holds configuration options for creating a WorkerPool instance.
type WorkerPoolConfig struct {
	// Name identifies this pool in metrics and logs.
	// Optional. Default is "workerpool".
	Name string
	// NumWorkers specifies the number of concurrent worker goroutines.
	// Must be at least 1. Default is 1.
	NumWorkers int
	// TaskQueueSize specifies the buffer capacity of the task queue.
	// Must be at least 1. Default is 1.
	TaskQueueSize int
	// Observer receives read-only callbacks for worker pool lifecycle events.
	// Use this for logging, metrics collection, or monitoring.
	// Optional. Can be nil if no observation is needed.
	Observer WorkerPoolObserver
	// ErrorHandler is called when a task returns an error.
	// Use this for error handling logic (retry, logging, alerting, etc.).
	// Optional. Can be nil if no error handling is needed.
	ErrorHandler func(workerID int, taskID string, err error)
	// PanicHandler is called when a task panics.
	// Use this for panic handling logic (recovery, alerting, etc.).
	// Optional. Can be nil if no panic handling is needed.
	PanicHandler func(workerID int, taskID string, panicValue any)
}

// Option is a functional option for configuring a WorkerPool instance.
// Options are passed to NewWorkerPool to customize behavior.
type Option func(*WorkerPoolConfig)

// WorkerPool manages a fixed-size pool of workers that process tasks concurrently.
// It provides task queuing, graceful shutdown, error handling, and metrics collection.
// The pool is generic and can process any task type that implements the Task interface.
//
// WorkerPool respects context cancellation and will gracefully shut down when
// the context is cancelled.
type WorkerPool[T Task[T]] struct {
	cfg       *WorkerPoolConfig
	taskQueue chan T
	metrics   *metrics
	errgroup  *errgroup.Group
	once      sync.Once
	ctx       context.Context
}

// metrics holds internal metrics for the worker pool.
// This type is internal only and not exposed.
type metrics struct {
	mu             sync.RWMutex
	TasksProcessed map[string]int // [worker_name] -> count
	TasksFailed    int
	ActiveWorkers  int
}

// MetricsSnapshot provides a point-in-time view of worker pool metrics.
// It contains counts of processed tasks per worker, total failed tasks,
// and the number of active workers. This is a safe copy that can be
// inspected without holding locks.
type MetricsSnapshot struct {
	TasksProcessed map[string]int // [worker_name] -> count
	TasksFailed    int
	ActiveWorkers  int
}

// NewWorkerPool creates a new WorkerPool instance with the provided options.
// It returns an error if the configuration is invalid (e.g., NumWorkers < 1
// or TaskQueueSize < 1).
//
// Options can be used to configure the pool name, number of workers,
// task queue size, and attach an observer:
//
//	pool, err := workerpool.NewWorkerPool[MyTask](ctx,
//	    workerpool.WithName("api-processor"),
//	    workerpool.WithNumWorkers(4),
//	    workerpool.WithTaskQueueSize(100),
//	    workerpool.WithObserver(myObserver),
//	)
func NewWorkerPool[T Task[T]](ctx context.Context, opts ...Option) (*WorkerPool[T], error) {
	cfg := &WorkerPoolConfig{
		Name:          "workerpool",
		NumWorkers:    1,
		TaskQueueSize: 1,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.NumWorkers < 1 {
		return nil, fmt.Errorf("workerpool: number of workers must be at least 1, got %d", cfg.NumWorkers)
	}

	if cfg.TaskQueueSize < 1 {
		return nil, fmt.Errorf("workerpool: task queue size must be at least 1, got %d", cfg.TaskQueueSize)
	}

	errgroup, ctx := errgroup.WithContext(ctx)

	return &WorkerPool[T]{
		cfg:       cfg,
		taskQueue: make(chan T, cfg.TaskQueueSize),
		ctx:       ctx,
		errgroup:  errgroup,
		metrics: &metrics{
			TasksProcessed: make(map[string]int),
		},
	}, nil
}

// WithName sets the name of the worker pool.
// The name is used in metrics and log messages to identify this pool.
// Optional. Default is "workerpool".
//
// Returns an Option that can be passed to NewWorkerPool.
func WithName(name string) Option {
	return func(cfg *WorkerPoolConfig) {
		cfg.Name = name
	}
}

// WithNumWorkers sets the number of concurrent worker goroutines.
// Must be at least 1. Default is 1.
//
// Returns an Option that can be passed to NewWorkerPool.
func WithNumWorkers(n int) Option {
	return func(cfg *WorkerPoolConfig) {
		cfg.NumWorkers = n
	}
}

// WithTaskQueueSize sets the buffer capacity of the task queue.
// Must be at least 1. Default is 1. Larger values allow more tasks to be
// queued before Push() blocks or returns an error.
//
// Returns an Option that can be passed to NewWorkerPool.
func WithTaskQueueSize(size int) Option {
	return func(cfg *WorkerPoolConfig) {
		cfg.TaskQueueSize = size
	}
}

// WithObserver attaches a WorkerPoolObserver to receive lifecycle event callbacks.
// Observer is for read-only monitoring (logging, metrics, observability).
// The observer's methods are called synchronously when workers start/stop
// or when tasks are received/completed/failed. Pass nil to disable observation (default).
//
// Returns an Option that can be passed to NewWorkerPool.
func WithObserver(observer WorkerPoolObserver) Option {
	return func(cfg *WorkerPoolConfig) {
		cfg.Observer = observer
	}
}

// WithErrorHandler sets a callback for handling task execution errors.
// Use this for error handling logic such as retry, alerting, or custom logging.
// This is different from Observer.OnTaskFailed which is for read-only monitoring.
//
// Returns an Option that can be passed to NewWorkerPool.
func WithErrorHandler(handler func(workerID int, taskID string, err error)) Option {
	return func(cfg *WorkerPoolConfig) {
		cfg.ErrorHandler = handler
	}
}

// WithPanicHandler sets a callback for handling task panics.
// Use this for panic handling logic such as alerting, recovery, or custom logging.
// This is different from Observer.OnTaskPanic which is for read-only monitoring.
//
// Returns an Option that can be passed to NewWorkerPool.
func WithPanicHandler(handler func(workerID int, taskID string, panicValue any)) Option {
	return func(cfg *WorkerPoolConfig) {
		cfg.PanicHandler = handler
	}
}

// Start launches the configured number of worker goroutines to begin processing tasks.
// Each worker runs independently and will process tasks from the queue until Stop is called
// or the context is cancelled. This method does not block; workers run in the background.
// Returns nil on successful startup.
func (w *WorkerPool[T]) Start() error {
	for i := 0; i < w.cfg.NumWorkers; i++ {
		workerID := i
		w.errgroup.Go(func() error {
			w.metrics.mu.Lock()
			w.metrics.ActiveWorkers++
			w.metrics.mu.Unlock()

			if w.cfg.Observer != nil {
				w.cfg.Observer.OnWorkerStart(workerID)
			}

			defer func() {
				w.metrics.mu.Lock()
				w.metrics.ActiveWorkers--
				w.metrics.mu.Unlock()

				if w.cfg.Observer != nil {
					w.cfg.Observer.OnWorkerStop(workerID)
				}
			}()

			return w.worker(workerID)
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
		return fmt.Errorf("workerpool %q: task queue is full (capacity: %d)", w.cfg.Name, cap(w.taskQueue))
	}
}

// Stop gracefully shuts down the worker pool by closing the task queue.
// This signals all workers to finish processing their current tasks and exit.
// Call Wait after Stop to block until all workers have completed.
// Safe to call multiple times.
func (w *WorkerPool[T]) Stop() {
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
// processing their current tasks.
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
func (w *WorkerPool[T]) worker(workerID int) error {
	for {
		select {
		case task, ok := <-w.taskQueue:
			if !ok {
				return nil
			}
			w.executeSafe(workerID, task)
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}
}

// executeSafe executes a task with panic recovery and error handling.
// It ensures that panics or errors during task execution don't crash the worker.
// Failures are recorded in metrics, and if configured, the observer is invoked.
// This implements a resilient pattern where individual task failures don't affect
// other tasks or workers, similar to Promise.allSettled in JavaScript.
func (w *WorkerPool[T]) executeSafe(workerID int, task T) {
	workerName := fmt.Sprintf("%s.worker.%d", w.cfg.Name, workerID)
	taskID := task.GetID()

	if w.cfg.Observer != nil {
		w.cfg.Observer.OnTaskReceived(workerID, taskID)
	}

	defer func() {
		if r := recover(); r != nil {
			w.recordTaskFailed()
			if w.cfg.Observer != nil {
				w.cfg.Observer.OnTaskPanic(workerID, taskID, r)
			}
			if w.cfg.PanicHandler != nil {
				w.cfg.PanicHandler(workerID, taskID, r)
			}
		}
	}()

	if err := task.Execute(w.ctx); err != nil {
		w.recordTaskFailed()
		if w.cfg.Observer != nil {
			w.cfg.Observer.OnTaskFailed(workerID, taskID, err)
		}
		if w.cfg.ErrorHandler != nil {
			w.cfg.ErrorHandler(workerID, taskID, err)
		}
		return
	}

	w.recordTaskSuccess(workerName)
	if w.cfg.Observer != nil {
		w.cfg.Observer.OnTaskCompleted(workerID, taskID)
	}
}

// recordTaskSuccess increments the task processed count for the specified worker.
func (w *WorkerPool[T]) recordTaskSuccess(workerName string) {
	w.metrics.mu.Lock()
	defer w.metrics.mu.Unlock()
	w.metrics.TasksProcessed[workerName]++
}

// recordTaskFailed increments the total count of failed tasks.
func (w *WorkerPool[T]) recordTaskFailed() {
	w.metrics.mu.Lock()
	defer w.metrics.mu.Unlock()
	w.metrics.TasksFailed++
}
