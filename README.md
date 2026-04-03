![Quacker](https://github.com/user-attachments/assets/7af9dc11-083c-4036-aae6-4f71331abe1d)

<h1 align="center">Quacker</h1>

<p align="center">
  <strong>Type-safe, composable concurrency patterns with backpressure for Go</strong>
</p>

<p align="center">
  <a href="https://github.com/SangTran-127/quacker/actions/workflows/ci.yml"><img src="https://github.com/SangTran-127/quacker/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pkg.go.dev/github.com/SangTran-127/quacker"><img src="https://pkg.go.dev/badge/github.com/SangTran-127/quacker.svg" alt="Go Reference"></a>
  <a href="https://codecov.io/github/SangTran-127/quacker"><img src="https://codecov.io/github/SangTran-127/quacker/graph/badge.svg?token=3YSWJQTMB7" alt="Coverage"></a>
  <a href="https://goreportcard.com/report/github.com/SangTran-127/quacker"><img src="https://goreportcard.com/badge/github.com/SangTran-127/quacker" alt="Go Report Card"></a>
  <img src="https://img.shields.io/badge/Made%20with-Go-1f425f.svg" alt="Made with Go">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
</p>

**Quacker** provides composable, channel-native concurrency primitives for Go. Backpressure propagates naturally through Go's channel semantics — no custom protocols, no framework lock-in.

## Installation

```bash
go get github.com/SangTran-127/quacker
```

**Requirements:** Go 1.25+

## Packages

### [`pipeline`](https://pkg.go.dev/github.com/SangTran-127/quacker/pipeline) — Stream processing with backpressure

Compose stages into pipelines. Each stage transforms a channel into a channel. Backpressure is free — when a downstream stage is slow, upstream blocks automatically.

```go
ctx, cancel := context.WithCancelCause(ctx)

out := pipeline.Run(ctx, input,
    pipeline.Map(3, func(ctx context.Context, e Event) (Event, bool) {
        enriched, err := enrich(ctx, e)
        if err != nil {
            cancel(err)
            return e, false
        }
        return enriched, true
    }),
    pipeline.Filter(func(e Event) bool { return e.Priority > 0 }),
    pipeline.Sink(5, func(ctx context.Context, e Event) {
        store(ctx, e)
    }),
)

for range out {}
if err := context.Cause(ctx); err != nil {
    log.Fatal(err)
}
```

**Stages:** `Map`, `Filter`, `ForEach`, `Buffer`, `Tee`, `Take`, `Sink`, `Batch`

### [`fanout`](https://pkg.go.dev/github.com/SangTran-127/quacker/fanout) — 1-to-N distribution

Distribute a single stream to multiple consumers. `RoundRobin` for load balancing, `Broadcast` for pub/sub.

```go
fo, _ := fanout.NewFanOut[int](
    fanout.WithWorkerCount(3),
    fanout.WithStrategy(fanout.Broadcast),
)
fo.Run(ctx, input)

for _, ch := range fo.Outputs() {
    go consume(ch)
}
```

### [`fanin`](https://pkg.go.dev/github.com/SangTran-127/quacker/fanin) — N-to-1 merge

Merge multiple channels into a single stream.

```go
fi, _ := fanin.NewFanIn[int]()
fi.Add(stream1)
fi.Add(stream2)
fi.Add(stream3)
merged := fi.Run(ctx)
```

### [`workerpool`](https://pkg.go.dev/github.com/SangTran-127/quacker/workerpool) — Bounded task processing

Fixed-size worker pool with push-based task queuing, metrics, and graceful shutdown.

```go
pool, _ := workerpool.NewWorkerPool[MyTask](ctx,
    workerpool.WithNumWorkers(4),
    workerpool.WithTaskQueueSize(100),
)
pool.Start()
pool.Push(task)
pool.StopAndWait()
```

## How they compose

The primitives connect through channels — Go's universal connector:

```
                    +-- pipeline --+
source --> fanout --+-- pipeline --+-- fanin --> pipeline --> sink
                    +-- pipeline --+
```

```go
fo.Run(ctx, source)

fast := pipeline.Run(ctx, fo.Outputs()[0], pipeline.Map(10, enrichFast))
slow := pipeline.Run(ctx, fo.Outputs()[1], pipeline.Map(2, enrichSlow))

fi.Add(fast)
fi.Add(slow)
merged := fi.Run(ctx)

out := pipeline.Run(ctx, merged, pipeline.Sink(5, store))
```

## Design

- **Backpressure is free.** Channels block when full. No custom protocol.
- **Errors use stdlib.** `context.WithCancelCause` + `context.Cause`. No custom error types.
- **Shutdown is clear.** Close input for graceful drain. Cancel context for immediate stop.
- **Small interfaces.** Observer interfaces are 2-3 methods, always optional.
- **No dependencies.** Core logic is stdlib only (`golang.org/x/sync` for errgroup in workerpool).

---
<p align="center">
  Built with Go
</p>
