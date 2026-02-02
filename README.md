<p align="center">
  <img src="https://github.com/user-attachments/assets/7af9dc11-083c-4036-aae6-4f71331abe1d" width="400" alt="Quacker">
</p>

<h1 align="center">Quacker</h1>

<p align="center">
  <strong>Type-safe, composable, and observable concurrency patterns for Go</strong>
</p>

<p align="center">
  <a href="https://github.com/SangTran-127/quacker/actions/workflows/ci.yml"><img src="https://github.com/SangTran-127/quacker/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pkg.go.dev/github.com/SangTran-127/quacker"><img src="https://pkg.go.dev/badge/github.com/SangTran-127/quacker.svg" alt="Go Reference"></a>
  <a href="https://codecov.io/github/SangTran-127/quacker"><img src="https://codecov.io/github/SangTran-127/quacker/graph/badge.svg?token=3YSWJQTMB7" alt="Coverage"></a>
  <a href="https://goreportcard.com/report/github.com/SangTran-127/quacker"><img src="https://goreportcard.com/badge/github.com/SangTran-127/quacker" alt="Go Report Card"></a>
  <img src="https://img.shields.io/badge/Made%20with-Go-1f425f.svg" alt="Made with Go">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
</p>

**Quacker** provides a suite of generic, high-performance concurrency primitives designed for modern Go applications. It simplifies complex concurrent workflows while ensuring type safety and observability.

## Installation

```bash
go get github.com/SangTran-127/quacker
```

**Requirements:** Go 1.25+

## Quick Guide

- Use [`fanout.FanOut`](https://pkg.go.dev/github.com/SangTran-127/quacker/fanout) to distribute work from a single stream to **multiple consumers**. Supports efficient `RoundRobin` load balancing or `Broadcast` messaging.
- Use [`fanin.FanIn`](https://pkg.go.dev/github.com/SangTran-127/quacker/fanin) to **merge multiple channels** into a single cohesive stream.
- Use [`workerpool.WorkerPool`](https://pkg.go.dev/github.com/SangTran-127/quacker/workerpool) for **bounded concurrency** when you need to process tasks with limited resources. Includes built-in task queuing, metrics tracking, and graceful shutdown.

## Highlights

- **Modern Go**: Leverages Go 1.25 features and strict type safety with generics.
- **Observable**: First-class support for `Observer` interfaces (metrics, logging, tracing) and error/panic handlers.
- **Robust**: Includes panic recovery, context cancellation propagation, and safe resource cleanup by default.
- **Zero Dependencies**: Core logic depends only on the standard library (test deps excluded).

## Benchmarks

> **Coming Soon**
> 
> Benchmarks against standard library implementations are currently being developed. Preliminary results show minimal overhead with significant usability gains.

---
<p align="center">
  Built with ❤️ in <a href="https://go.dev/">Go</a> by <a href="https://github.com/SangTran-127">Sang Tran</a>
</p>
