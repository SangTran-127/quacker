# Quacker — Project Instructions

## Project Purpose

This is a **learning project** for advanced Go concurrency patterns. The goal is deep understanding, not vibe coding. Every design decision should be intentional and defensible.

## AI Behavior

- **Never generate code without explaining the concurrency reasoning behind it.**
- **Always review code after writing it** — run `go vet`, `staticcheck`, and `go test -race` before considering anything done.
- **Challenge my design.** If something is over-engineered or a stdlib solution is simpler, say so. If I'm missing an edge case, point it out.
- **Teach, don't just fix.** When you find a bug, explain *why* it's wrong in concurrency terms (race, deadlock, goroutine leak, etc).
- **No silent panic recovery** unless I explicitly ask for it at a boundary. Panics are bugs — surface them.

## Architecture

- `fanout/` — 1-to-N channel distribution (RoundRobin, Broadcast)
- `fanin/` — N-to-1 channel merging
- `workerpool/` — Bounded concurrency task processing
- Future: pipeline composition, backpressure, adaptive concurrency

## Go Best Practices — Enforced

### Concurrency Rules

1. **Channel ownership:** Whoever creates a channel is responsible for closing it. Never close a channel from the receiving side.
2. **Goroutine lifecycle:** Every goroutine must have a clear exit condition. Document it. If you can't explain when a goroutine exits, don't spawn it.
3. **No goroutine leaks:** Every `go func()` must be reachable by context cancellation or channel close. Use the double-select pattern when forwarding between channels:
   ```go
   select {
   case out <- v:
   case <-ctx.Done():
       return
   }
   ```
4. **Nil channel awareness:** Reading from a nil channel blocks forever. Always validate channel parameters at public API boundaries.
5. **Synchronization choice:** Use channels for communication, mutexes for state. Don't use mutexes to orchestrate goroutine flow. Don't use channels as locks.
6. **WaitGroup discipline:** `Add()` must happen before the goroutine starts, or use `wg.Go()`. Never call `Add()` inside the goroutine.
7. **Context propagation:** All long-running operations must accept and respect `context.Context`. Never ignore cancellation.

### Error Handling

8. **Errors are values.** Use sentinel errors (`var ErrQueueFull = errors.New(...)`) or typed errors for anything callers need to handle programmatically. Never force string matching.
9. **Panics are bugs, not errors.** Only recover at explicit boundaries (HTTP handlers, plugin loaders). Document why when you do.
10. **Wrap errors with context:** `fmt.Errorf("workerpool: push: %w", err)` — always include the package/operation.

### API Design

11. **Small interfaces.** 1-3 methods max. If an interface has 5+ methods, split it or use callback functions.
12. **Make the zero value useful** where possible. If not possible, enforce construction via `NewX()`.
13. **Functional options for configuration.** Validate in the constructor, not at runtime.
14. **Enforce single-use semantics** with `atomic.Bool` for methods that must only be called once (Run, Start).
15. **Return receive-only `<-chan T`** from public APIs to prevent callers from closing or sending.

### Code Quality

16. **No variable shadowing of imports.** Rename the local variable, not the import.
17. **Pre-compute values outside hot loops.** Don't `fmt.Sprintf` on every task execution.
18. **`count++` not `count += 1`.** Follow Go idioms.
19. **If a function signature promises an error but never returns one, remove the error return.** Don't lie in the API.
20. **Exported types need godoc comments.** Internal types don't need novels.

### Testing

21. **No `time.Sleep` in tests for synchronization.** Use channels, WaitGroups, or condition variables. `time.Sleep` is a flaky test waiting to happen.
22. **Every assertion needs a useful error message.** `t.Fatal("fo error")` is unacceptable.
23. **Test edge cases:** nil inputs, double calls, context already cancelled, zero-value configs.
24. **Use `t.Parallel()` on all tests that don't share mutable state.**
25. **Run with `-race` always.** CI must include `-race`. No exceptions.

### Design Principles (Rob Pike School)

26. **"A little copying is better than a little dependency."** Don't add a dependency for something you can write in 10 lines.
27. **"Clear is better than clever."** If you need a comment to explain a goroutine's purpose, the code isn't clear enough.
28. **"Don't communicate by sharing memory; share memory by communicating."** Default to channels. Mutexes are for protecting state, not for coordination.
29. **"The bigger the interface, the weaker the abstraction."** If your interface has 6 methods, you're probably designing a class, not an interface.
30. **Earn your abstraction.** Every type, method, and option must justify its existence against "would the user rather write this with stdlib?" If yes, your abstraction isn't pulling its weight.

## Senior-Level: Runtime & Memory Model

Understanding *what* to write is mid-level. Understanding *why* it works at the runtime level is senior.

### Go Scheduler (GMP Model)

31. **Know the GMP model.** G = goroutine, M = OS thread, P = logical processor. Goroutines are multiplexed onto OS threads. `GOMAXPROCS` controls the number of Ps, not the number of threads. When a goroutine blocks on a syscall, the M is parked and a new M is spun up — this is why goroutines are cheap but syscall-heavy code still needs bounded concurrency.
32. **Goroutines are cooperatively scheduled (mostly).** Preemption happens at function call boundaries and since Go 1.14, at async preemption points via signals. A tight `for {}` loop with no function calls can starve other goroutines on the same P. This matters when writing compute-heavy workers — add `runtime.Gosched()` or ensure function calls exist in hot loops.
33. **Know when goroutines yield.** Channel operations, mutex locks, syscalls, `runtime.Gosched()`, function calls (preemption points). A goroutine that never hits these points monopolizes its P.

### Memory Model

34. **Happens-before is the only guarantee.** Go's memory model guarantees visibility only through synchronization events: channel send/receive, mutex lock/unlock, `sync.Once.Do`, atomic operations. Without these, one goroutine's write may never be visible to another — even on x86 where it "usually works." Never rely on timing or memory ordering without explicit synchronization.
35. **`sync/atomic` provides ordering guarantees, not just atomicity.** `atomic.Store` followed by `atomic.Load` on the same variable establishes happens-before. But atomic operations on *different* variables don't order each other. Use `atomic` for flags and counters, not for coordinating access to unrelated state.
36. **Understand false sharing.** When two goroutines write to adjacent memory (e.g., struct fields on the same cache line), the CPU bounces the cache line between cores. For high-contention counters, pad with `_ [64]byte` or use per-goroutine accumulation. The standard library's `sync.Pool` does this internally.

### Stack & Heap

37. **Know escape analysis.** `go build -gcflags='-m'` shows what escapes to the heap. If a variable's lifetime exceeds the function (returned, captured by a goroutine, stored in an interface), it escapes. Heap allocation means GC pressure. In hot paths, keep allocations on the stack by avoiding unnecessary pointers, interfaces, and closures.
38. **Goroutine stacks start small (2-8KB) and grow.** This is why spawning 100k goroutines is fine on memory — but each one that holds a reference to a large buffer pins that buffer in memory. Be mindful of what goroutines capture in closures.

## Senior-Level: Performance Engineering

Correct code that's slow is a prototype, not a product.

### Profiling

39. **Profile before optimizing.** Use `pprof` for CPU, memory, goroutine, and mutex contention profiling. `go test -cpuprofile=cpu.out -memprofile=mem.out -bench=.` then `go tool pprof`. Never guess where the bottleneck is.
40. **Benchmark properly.** Use `testing.B` with `b.ResetTimer()`, `b.ReportAllocs()`, `b.RunParallel()`. A benchmark without `b.ReportAllocs()` is incomplete. Compare before/after with `benchstat`. Never trust a single benchmark run.
41. **Trace for latency analysis.** `go tool trace` shows goroutine scheduling, GC pauses, syscall blocks, and network polling in a timeline. This is how you debug "my service is slow but CPU is low" — it's usually goroutine contention or GC stop-the-world pauses.

### Allocation Discipline

42. **Zero-allocation hot paths.** In code that runs per-request or per-task, avoid: `fmt.Sprintf` (allocates), interface conversions (may allocate), closures that capture variables (allocates), slice append beyond capacity (allocates). Use `sync.Pool` for reusable buffers. Pre-allocate slices with `make([]T, 0, knownCap)`.
43. **Understand `sync.Pool` tradeoffs.** Pool reduces allocation but objects can be collected at any GC cycle. Never store state in pooled objects that must persist. Good for: byte buffers, temporary structs. Bad for: connection pools (use a channel-based pool instead).
44. **Avoid `interface{}` / `any` in hot paths.** Boxing a value into an interface causes a heap allocation. Generics (since Go 1.18) eliminate this for type-safe code. Your use of generics in Quacker is the right approach.

### Concurrency Performance

45. **Know lock contention patterns.** `RWMutex` helps when reads >> writes, hurts when writes are frequent (readers starve writers). For highly contended counters, consider `atomic.Int64` or per-goroutine sharding with periodic aggregation. Profile with `go tool pprof -contentionz`.
46. **Channel size is a design decision, not a tuning knob.** Unbuffered = synchronization point (rendezvous). Buffered(1) = decouple producer/consumer by one step. Buffered(N) = absorb burst, but oversizing masks backpressure. If you can't justify the buffer size, use 0.
47. **`select` with `default` is non-blocking, without `default` is blocking.** This distinction drives your `Push()` design — `default` makes it non-blocking. Know when each is appropriate. Non-blocking select in a loop without sleep burns CPU.

## Principal-Level: System Design & Patterns

Principal-level is about designing systems that other senior engineers can build, maintain, and evolve.

### Advanced Concurrency Patterns

48. **Singleflight** (`golang.org/x/sync/singleflight`). When N goroutines request the same resource simultaneously, only one does the work, others wait for the result. Essential for cache stampede prevention. Know when to use it vs. `sync.Once` (one-time init) vs. channel-based dedup.
49. **Semaphore pattern.** Bounded concurrency without a worker pool. `make(chan struct{}, N)` as a semaphore — acquire by sending, release by receiving. Simpler than a full worker pool when you just need concurrency limiting. Know when a semaphore is enough and a worker pool is overkill.
50. **Pipeline cancellation and draining.** When a pipeline stage fails, you must: (1) signal upstream to stop producing, (2) drain downstream to unblock upstream senders, (3) propagate errors. Getting all three right is principal-level work. Most implementations get 1 or 2 but not all 3.
51. **Rate limiting with token bucket / leaky bucket.** `golang.org/x/time/rate` implements token bucket. Know the difference: token bucket allows bursts up to bucket size, leaky bucket enforces constant rate. Choose based on whether your downstream tolerates bursts.
52. **Circuit breaker pattern.** Closed (normal) → Open (failing, fail fast) → Half-Open (test recovery). Prevents cascade failures in distributed systems. Know when to implement at the client vs. infrastructure layer (service mesh).

### Graceful Lifecycle Management

53. **Shutdown ordering matters.** Stop accepting new work → drain inflight work → close downstream connections → flush logs/metrics → exit. Getting this wrong means lost data or hanging connections. `signal.NotifyContext` for OS signals, then ordered teardown.
54. **Health check design.** Liveness (process alive?) vs. Readiness (can accept traffic?). A worker pool should report not-ready when queue is full, not-live when a worker goroutine has panicked and not recovered. These map directly to Kubernetes probes.

### API Design at Scale

55. **Backward compatibility contract.** Once you export a type, function, or method — it's a promise. Adding a method to an exported interface breaks all implementors. This is why Go 1.x has never broken compatibility. Use functional options so you can add new options without breaking callers.
56. **Accept interfaces, return structs.** Take `io.Reader` as input, return `*MyType` as output. This maximizes flexibility for callers while maintaining your freedom to change internals. Exception: return interfaces when the concrete type should be opaque.
57. **Design for `go doc`.** Your package godoc is your primary API documentation. Structure it with package-level doc, example functions (`Example_xxx`), and testable examples in `_test.go`. Users read `go doc`, not your README.

### Observability in Production

58. **Structured logging with `log/slog`.** Not `fmt.Println`, not `log.Printf`. Structured key-value pairs that can be parsed by log aggregators. `slog.Info("task completed", "worker_id", id, "duration", elapsed)`.
59. **OpenTelemetry for tracing.** Traces show request flow across goroutines and services. A pipeline library should propagate trace context through stages so users can see "this task spent 50ms in fan-out, 200ms in worker, 10ms in fan-in."
60. **Metrics with labels, not per-instance counters.** Your `TasksProcessed map[string]int` is per-worker — that's fine internally. But expose metrics with labels: `tasks_total{pool="api", status="success"}`. This integrates with Prometheus/OTel natively.

## Review Checklist (Run After Every Change)

### Correctness (must pass)
```
- [ ] `go vet ./...` passes
- [ ] `staticcheck ./...` passes
- [ ] `go test ./... -race -count=1` passes
- [ ] No goroutine can leak (every goroutine has a ctx.Done or channel-close exit)
- [ ] No panic can deadlock a WaitGroup or leave channels unclosed
- [ ] Observer/callback panics are handled or documented as caller's responsibility
- [ ] Errors are sentinel or typed, not string-formatted
- [ ] Interfaces are ≤3 methods
- [ ] No time.Sleep in tests for synchronization
- [ ] Public APIs validate inputs at the boundary
```

### Performance (check on hot paths)
```
- [ ] `go test -bench=. -benchmem` — know your allocations
- [ ] `go build -gcflags='-m' 2>&1 | grep 'escapes'` — review escape analysis on hot paths
- [ ] No fmt.Sprintf, interface boxing, or closure capture in per-task code
- [ ] Channel buffer sizes are justified, not arbitrary
- [ ] Lock contention is bounded (prefer atomic or sharding for counters)
```

### Design (check before committing public API)
```
- [ ] Can a user achieve the same thing with ≤15 lines of stdlib? If yes, earn the abstraction
- [ ] New exported types have godoc and at least one testable Example
- [ ] Accept interfaces, return structs
- [ ] No method added to existing exported interfaces (breaks implementors)
- [ ] Shutdown path tested: drain inflight → reject new → timeout → force exit
```
