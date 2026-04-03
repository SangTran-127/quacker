# Quacker Project Review

**Reviewed:** 2026-03-11
**Scope:** All source files, tests, CI, and documentation
**Tools:** `go vet`, `staticcheck`, `go test -race` — all pass clean

---

## Summary

Quacker is a well-written Go concurrency library with solid design patterns. The code demonstrates strong understanding of Go concurrency, proper use of generics, and good API design via functional options. Below are all identified issues ranked by severity.

---

## Critical Issues

_None found._

---

## Medium Issues

### 1. FanOut: No panic recovery in dispatcher goroutine
**File:** `fanout/fanout.go:209-231`

The dispatcher goroutine in `Run()` has no `recover()`. If an observer's `OnDistribute()` panics, the goroutine dies silently — output channels never close, and all consumers hang forever.

```go
// Current: no recover
go func() {
    idx := 0
    defer f.running.Store(false)
    defer f.closeAllOutput()
    for { ... }
}()
```

**Fix:** Add `defer func() { if r := recover(); r != nil { /* log/handle */ } }()` before the existing defers, or wrap observer calls in a recovery helper.

---

### 2. FanOut: Observer panic in `broadcast()` leaks goroutines
**File:** `fanout/fanout.go:285-302`

In `broadcast()`, each goroutine calls `f.cfg.Observer.OnDistribute()` after sending. If the observer panics, the goroutine dies without decrementing the WaitGroup (via `wg.Go`), causing `wg.Wait()` to hang, which blocks the dispatcher forever.

```go
wg.Go(func() {
    select {
    case ch <- value:
        if f.cfg.Observer != nil {
            f.cfg.Observer.OnDistribute() // panic here = deadlock
        }
    case <-ctx.Done():
        return
    }
})
```

**Fix:** Wrap observer calls with panic recovery inside the goroutine.

---

### 3. FanIn: Observer panic in `Run()` goroutines causes hang
**File:** `fanin/fanin.go:168-194`

Same pattern as FanOut. If `Observer.OnInputClosed()` panics inside a `wg.Go` goroutine, the WaitGroup never completes and the output channel never closes.

**Fix:** Add panic recovery around observer calls.

---

### 4. WorkerPool: `Start()` can be called multiple times
**File:** `workerpool/workerpool.go:273-299`

Unlike `FanOut.Run()` and `FanIn.Run()` which enforce single invocation via `atomic.Bool`, `WorkerPool.Start()` has no guard. Calling `Start()` twice doubles the number of workers without updating config, leading to unexpected behavior and incorrect `ActiveWorkers` metrics.

**Fix:** Add an `atomic.Bool` guard like the other packages:
```go
if !w.started.CompareAndSwap(false, true) {
    panic("workerpool: Start() called multiple times")
}
```

---

## Low Issues

### 5. FanOut test: Dead code assertion
**File:** `fanout/fanout_test.go:340`

```go
if receivedCount < 0 {
    t.Fatal("fo error, received count must greater than 0")
}
```
`receivedCount` is `len(received)` which can never be negative. This check is dead code — likely intended to be `receivedCount == 0` or removed entirely.

---

### 6. FanOut test: Weak assertion
**File:** `fanout/fanout_test.go:344-346`

```go
if receivedCount > 10 {
    t.Fatal("fo error")
}
```
Error message `"fo error"` provides no context. The test cancels the context immediately after a 10ms sleep with no values sent to the input, so `receivedCount` should always be 0. The assertion is both misleading and unhelpful for debugging.

---

### 7. WorkerPool: String allocation per task execution
**File:** `workerpool/workerpool.go:388`

```go
workerName := fmt.Sprintf("%s.worker.%d", w.cfg.Name, workerID)
```
This allocates a new string on every task execution. For high-throughput pools, this creates unnecessary GC pressure.

**Fix:** Pre-compute worker names in `Start()` and pass them to the worker loop.

---

### 8. No nil channel validation in `FanIn.Add()`
**File:** `fanin/fanin.go:122-135`

Adding a nil channel silently causes the corresponding goroutine to block forever on `<-ch` (reading from nil channel blocks permanently), preventing the WaitGroup from completing — the output channel never closes.

**Fix:**
```go
if ch == nil {
    panic("fanin: cannot add nil channel")
}
```

---

### 9. No nil input validation in `FanOut.Run()`
**File:** `fanout/fanout.go:204`

Passing a nil input channel causes the dispatcher to block forever on `<-input` (reading from nil blocks permanently). The outputs never close.

**Fix:**
```go
if input == nil {
    panic("fanout: input channel must not be nil")
}
```

---

### 10. WorkerPool test: Timing-dependent with `time.Sleep`
**Files:** `workerpool/workerpool_test.go:185,236,277,627`

Multiple tests use `time.Sleep(50 * time.Millisecond)` to wait for task processing. This is fragile and can flake on slow CI runners or under load.

**Fix:** Use synchronization primitives (channels, WaitGroups) instead of sleep for deterministic tests.

---

### 11. WorkerPool test: Non-assertive test
**File:** `workerpool/workerpool_test.go:554-586` (`TestWorkerPool_PushContextCancelled`)

This test logs outcomes but never fails. All three code paths (push success, context cancelled, queue full) are treated as acceptable, making the test useless as a regression guard.

**Fix:** Assert the expected behavior explicitly.

---

### 12. Committed coverage artifact
**File:** `workerpool/cover.out`

The `cover.out` file is committed to the repository. This is a generated artifact and should be in `.gitignore`.

**Fix:** Add `cover.out` to `.gitignore` (it's already listed, but the file was committed before the rule was added). Remove it with `git rm workerpool/cover.out`.

---

### 13. `FanOutStrategy` accepts invalid values
**File:** `fanout/fanout.go:84-96,219`

`FanOutStrategy` is a plain `int`. Users can pass `FanOutStrategy(42)` and the `switch` in `Run()` silently does nothing — the value is consumed from input but never distributed to any output, with no error.

**Fix:** Add a `default` case in the switch:
```go
default:
    panic(fmt.Sprintf("fanout: unknown strategy %d", f.cfg.Strategy))
```
Or validate during `NewFanOut()`.

---

### 14. README: Claims "Zero Dependencies" but uses `golang.org/x/sync`
**File:** `README.md:39`

> Zero Dependencies: Core logic depends only on the standard library (test deps excluded).

`golang.org/x/sync` is used in `workerpool` production code (not just tests). While `x/sync` is quasi-stdlib, the claim is technically inaccurate.

---

## Informational / Style

### 15. Copyright year says 2025
**Files:** All source files

The copyright header says `Copyright (c) 2025` but Go 1.25 doesn't exist in 2025. This is cosmetic but could confuse readers. Either the year or the Go version reference is off — likely fine, just noting.

### 16. Inconsistent `BroadCast` vs `Broadcast` naming
**Files:** `fanout/fanout.go:95` vs doc comments and test names

The constant is `Broadcast` (correct Go style), but doc comments reference "BroadCast" in some places (e.g., line 23: `BroadCast`). The test function is `TestFanOut_RunBroadCast`. Should be consistent.

### 17. `errgroup` variable shadows import
**File:** `workerpool/workerpool.go:190`

```go
errgroup, ctx := errgroup.WithContext(ctx)
```
The local variable `errgroup` shadows the imported package name. While it compiles fine, it reduces readability.

---

## Test Coverage Gaps

| Scenario | Package |
|---|---|
| `Start()` called multiple times | workerpool |
| `Push()` after `Stop()` | workerpool |
| `Run()` with 0 inputs added | fanin |
| `Done()` called before `Run()` | fanin |
| Nil channel added to FanIn | fanin |
| Nil input passed to FanOut.Run() | fanout |
| Invalid `FanOutStrategy` value | fanout |
| Observer that panics | all packages |

---

## Verdict

The project is well-designed with clean APIs and solid concurrency patterns. The most impactful issues to fix are:

1. **Add panic recovery around all observer callbacks** (issues #1-3) — this is the biggest reliability gap
2. **Guard `WorkerPool.Start()` against multiple calls** (issue #4)
3. **Validate nil channels/inputs** (issues #8-9)
4. **Remove committed `cover.out`** (issue #12)

Everything else is polish. The core concurrency logic is correct — no races detected, `go vet` and `staticcheck` pass clean.
