package workerpool

import (
	"context"
	"fmt"
	"testing"
)

type benchTask struct {
	id string
}

func (t *benchTask) Execute(_ context.Context) error { return nil }
func (t *benchTask) GetID() string                   { return t.id }

func BenchmarkWorkerPool(b *testing.B) {
	b.ReportAllocs()

	pool, _ := NewWorkerPool[*benchTask](b.Context(),
		WithNumWorkers(4),
		WithTaskQueueSize(256),
	)
	pool.Start()

	task := &benchTask{id: "bench"}

	for b.Loop() {
		pool.Push(task)
	}

	pool.StopAndWait()
}

func BenchmarkWorkerPool_Push(b *testing.B) {
	b.ReportAllocs()

	pool, _ := NewWorkerPool[*benchTask](b.Context(),
		WithNumWorkers(8),
		WithTaskQueueSize(1024),
	)
	pool.Start()
	defer pool.StopAndWait()

	b.RunParallel(func(pb *testing.PB) {
		task := &benchTask{id: fmt.Sprintf("bench-%d", b.N)}
		for pb.Next() {
			pool.Push(task)
		}
	})
}
