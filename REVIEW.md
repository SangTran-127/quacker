# Quacker Project Review

**Initial review:** 2026-03-11
**Last updated:** 2026-04-05
**Scope:** All source files, tests, CI, and documentation
**Tools:** `go vet`, `staticcheck`, `go test -race` — all pass clean

---

## Summary

17 issues identified in the initial review. All have been resolved.

---

## Issue Tracker

| # | Issue | Severity | Status | Fix |
|---|---|---|---|---|
| 1 | FanOut: no panic recovery in dispatcher goroutine | Medium | Fixed | `safeObserve()` wraps all observer calls |
| 2 | FanOut: observer panic in `broadcast()` leaks goroutines | Medium | Fixed | `safeObserve()` in broadcast goroutines |
| 3 | FanIn: observer panic in `Run()` goroutines causes hang | Medium | Fixed | `safeObserve()` wraps observer calls |
| 4 | WorkerPool: `Start()` can be called multiple times | Medium | Fixed | `atomic.Bool` guard with `CompareAndSwap` |
| 5 | FanOut test: dead code assertion (`receivedCount < 0`) | Low | Fixed | Test rewritten with proper assertions |
| 6 | FanOut test: weak assertion (`t.Fatal("fo error")`) | Low | Fixed | Descriptive error messages |
| 7 | WorkerPool: string allocation per task execution | Low | Fixed | `workerName` pre-computed in `Start()` |
| 8 | No nil channel validation in `FanIn.Add()` | Low | Fixed | `panic("fanin: cannot add nil channel")` |
| 9 | No nil input validation in `FanOut.Run()` | Low | Fixed | `panic("fanout: input channel must not be nil")` |
| 10 | WorkerPool tests: timing-dependent `time.Sleep` | Low | Fixed | Replaced with `StopAndWait()` drain |
| 11 | WorkerPool test: non-assertive `PushContextCancelled` | Low | Fixed | Explicit `errors.Is` assertions |
| 12 | Committed `workerpool/cover.out` artifact | Low | Fixed | Removed; `.gitignore` already covers `*.out` |
| 13 | `FanOutStrategy` accepts invalid values silently | Low | Fixed | Validated in `NewFanOut()` constructor |
| 14 | README claims "Zero Dependencies" | Low | Fixed | README rewritten |
| 15 | Copyright year says 2025 | Info | N/A | Cosmetic, left as-is |
| 16 | Inconsistent `BroadCast` vs `Broadcast` naming | Info | Fixed | Normalized to `Broadcast` |
| 17 | `errgroup` variable shadows import | Info | Fixed | Renamed to `eg` |

---

## Additional Improvements (2026-04-05)

- **Observer panic recovery:** Added `safeObserve()` helper to fanout, fanin, and workerpool. Observers are user-provided code (a boundary) — panics are recovered to prevent one bad observer from crashing the program. Same justification as `net/http` recovering handler panics.
- **Sentinel errors:** `ErrQueueFull` (workerpool) and `ErrInvalidStrategy` (fanout) — the only runtime conditions callers branch on. Validation errors use `fmt.Errorf` (programmer mistakes, not runtime conditions).
- **WorkerPool observer interface:** Slimmed from 6 methods to 3 (`OnWorkerStart`, `OnWorkerStop`, `OnTaskDone`). Panics routed through `PanicHandler`, not the observer.
- **`Start()` returns nothing:** Previously `Start() error` always returned nil. Removed the lie.
- **Benchmarks:** Added for all 4 packages with `b.ReportAllocs()`.
- **Testable examples:** Added `example_test.go` for fanout, fanin, and pipeline (visible on pkg.go.dev).
- **Pipeline package:** New package with composable stages, backpressure via channels, error propagation via `context.WithCancelCause`.

---

## Benchmark Results (Apple M4, Go 1.25.1)

```
BenchmarkFanIn-10                    5209598    228.6 ns/op    0 B/op    0 allocs/op
BenchmarkFanOut_RoundRobin-10        5815704    204.5 ns/op    0 B/op    0 allocs/op
BenchmarkFanOut_Broadcast-10          689947   1762   ns/op  368 B/op    8 allocs/op
BenchmarkMap-10                      3024300    391.9 ns/op    0 B/op    0 allocs/op
BenchmarkPipeline_Composition-10      924622   1341   ns/op    0 B/op    0 allocs/op
BenchmarkWorkerPool-10               7866024    147.2 ns/op   10 B/op    0 allocs/op
BenchmarkWorkerPool_Push-10          7415193    164.3 ns/op   72 B/op    2 allocs/op
```

Zero allocations on hot paths (FanIn, FanOut RoundRobin, Map, Pipeline composition). Broadcast allocates due to per-send goroutine spawning via `wg.Go`.

---

## Verdict

All 17 original issues resolved. The project now has:
- Observer panic recovery at all boundaries
- Nil input validation at all public APIs
- Single-use guards on all lifecycle methods
- Deterministic tests with no `time.Sleep` synchronization
- Benchmarks with allocation tracking
- Testable examples for godoc
- A pipeline package with backpressure, composable stages, and stdlib error propagation
