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
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// ErrQueueFull is returned by Push when the task queue has no capacity.
// This is the only runtime error callers need to branch on.
var ErrQueueFull = errors.New("workerpool: task queue is full")

// Task represents a unit of work that can be executed by the worker pool.
type Task[T any] interface {
	Execute(ctx context.Context) error
	GetID() string
}

// WorkerPoolObserver defines callbacks for monitoring worker pool lifecycle events.
// Implementations should be lightweight to avoid blocking task processing.
// Observer methods must not panic — a panic will propagate and crash the program.
//
// OnTaskDone is called for success (err==nil) and error (err!=nil).
// Panics are not routed through the observer — use WithPanicHandler for those.
type WorkerPoolObserver interface {
	OnWorkerStart(workerID int)
	OnWorkerStop(workerID int)
	OnTaskDone(workerID int, taskID string, err error)
}

// WorkerPoolConfig holds configuration options for creating a WorkerPool instance.
type WorkerPoolConfig struct {
	Name          string
	NumWorkers    int
	TaskQueueSize int
	Observer      WorkerPoolObserver
	ErrorHandler  func(workerID int, taskID string, err error)
	PanicHandler  func(workerID int, taskID string, panicValue any)
}

// Option is a functional option for configuring a WorkerPool instance.
type Option func(*WorkerPoolConfig)

// WorkerPool manages a fixed-size pool of workers that process tasks concurrently.
type WorkerPool[T Task[T]] struct {
	cfg       *WorkerPoolConfig
	taskQueue chan T
	metrics   *metrics
	eg        *errgroup.Group
	once      sync.Once
	ctx       context.Context
	started   atomic.Bool
}

type metrics struct {
	mu             sync.RWMutex
	TasksProcessed map[string]int
	TasksFailed    int
	ActiveWorkers  int
}

// MetricsSnapshot provides a point-in-time view of worker pool metrics.
type MetricsSnapshot struct {
	TasksProcessed map[string]int
	TasksFailed    int
	ActiveWorkers  int
}

// NewWorkerPool creates a new WorkerPool with the provided options.
// Returns an error if NumWorkers < 1 or TaskQueueSize < 1.
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

	eg, ctx := errgroup.WithContext(ctx)

	return &WorkerPool[T]{
		cfg:       cfg,
		taskQueue: make(chan T, cfg.TaskQueueSize),
		ctx:       ctx,
		eg:        eg,
		metrics: &metrics{
			TasksProcessed: make(map[string]int),
		},
	}, nil
}

func WithName(name string) Option {
	return func(cfg *WorkerPoolConfig) { cfg.Name = name }
}

func WithNumWorkers(n int) Option {
	return func(cfg *WorkerPoolConfig) { cfg.NumWorkers = n }
}

func WithTaskQueueSize(size int) Option {
	return func(cfg *WorkerPoolConfig) { cfg.TaskQueueSize = size }
}

func WithObserver(observer WorkerPoolObserver) Option {
	return func(cfg *WorkerPoolConfig) { cfg.Observer = observer }
}

func WithErrorHandler(handler func(workerID int, taskID string, err error)) Option {
	return func(cfg *WorkerPoolConfig) { cfg.ErrorHandler = handler }
}

func WithPanicHandler(handler func(workerID int, taskID string, panicValue any)) Option {
	return func(cfg *WorkerPoolConfig) { cfg.PanicHandler = handler }
}

// Start launches worker goroutines. Must only be called once; panics on second call.
func (w *WorkerPool[T]) Start() {
	if !w.started.CompareAndSwap(false, true) {
		panic("workerpool: Start() called multiple times")
	}

	for i := 0; i < w.cfg.NumWorkers; i++ {
		workerID := i
		workerName := fmt.Sprintf("%s.worker.%d", w.cfg.Name, workerID)
		w.eg.Go(func() error {
			w.metrics.mu.Lock()
			w.metrics.ActiveWorkers++
			w.metrics.mu.Unlock()

			if w.cfg.Observer != nil {
				safeObserve(func() { w.cfg.Observer.OnWorkerStart(workerID) })
			}

			defer func() {
				w.metrics.mu.Lock()
				w.metrics.ActiveWorkers--
				w.metrics.mu.Unlock()

				if w.cfg.Observer != nil {
					safeObserve(func() { w.cfg.Observer.OnWorkerStop(workerID) })
				}
			}()

			return w.worker(workerID, workerName)
		})
	}
}

// Push adds a task to the queue. Returns ErrQueueFull (wrapped) if full,
// or the context error (wrapped) if the pool's context is cancelled.
func (w *WorkerPool[T]) Push(task T) error {
	select {
	case w.taskQueue <- task:
		return nil
	case <-w.ctx.Done():
		return fmt.Errorf("workerpool %q: push: %w", w.cfg.Name, w.ctx.Err())
	default:
		return fmt.Errorf("workerpool %q: capacity %d: %w", w.cfg.Name, cap(w.taskQueue), ErrQueueFull)
	}
}

// Stop closes the task queue. Safe to call multiple times.
func (w *WorkerPool[T]) Stop() {
	w.once.Do(func() {
		close(w.taskQueue)
	})
}

// Wait blocks until all workers finish.
func (w *WorkerPool[T]) Wait() error {
	return w.eg.Wait()
}

// StopAndWait closes the queue and waits for all workers to drain.
func (w *WorkerPool[T]) StopAndWait() error {
	w.Stop()
	return w.Wait()
}

// GetMetrics returns a snapshot of current pool metrics. Thread-safe.
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

func (w *WorkerPool[T]) worker(workerID int, workerName string) error {
	for {
		select {
		case task, ok := <-w.taskQueue:
			if !ok {
				return nil
			}
			w.executeSafe(workerID, workerName, task)
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}
}

func (w *WorkerPool[T]) executeSafe(workerID int, workerName string, task T) {
	taskID := task.GetID()

	defer func() {
		if r := recover(); r != nil {
			w.recordTaskFailed()
			if w.cfg.PanicHandler != nil {
				w.cfg.PanicHandler(workerID, taskID, r)
			}
		}
	}()

	if err := task.Execute(w.ctx); err != nil {
		w.recordTaskFailed()
		if w.cfg.Observer != nil {
			w.cfg.Observer.OnTaskDone(workerID, taskID, err)
		}
		if w.cfg.ErrorHandler != nil {
			w.cfg.ErrorHandler(workerID, taskID, err)
		}
		return
	}

	w.recordTaskSuccess(workerName)
	if w.cfg.Observer != nil {
		w.cfg.Observer.OnTaskDone(workerID, taskID, nil)
	}
}

func (w *WorkerPool[T]) recordTaskSuccess(workerName string) {
	w.metrics.mu.Lock()
	defer w.metrics.mu.Unlock()
	w.metrics.TasksProcessed[workerName]++
}

func (w *WorkerPool[T]) recordTaskFailed() {
	w.metrics.mu.Lock()
	defer w.metrics.mu.Unlock()
	w.metrics.TasksFailed++
}

// safeObserve calls an observer method, recovering if it panics.
// Observers are user-provided code — a boundary where recovery is justified.
func safeObserve(fn func()) {
	defer func() { recover() }()
	fn()
}
