// Copyright (c) 2025 Tran Quang Sang
// SPDX-License-Identifier: MIT

package pipeline

// StageObserver receives callbacks for stage lifecycle events.
// Implementations must be safe for concurrent use when attached
// to stages with concurrency > 1.
type StageObserver interface {
	OnItem()
	OnDrop()
	OnDone()
}

// StageOption configures a stage's behavior.
type StageOption func(*stageConfig)

type stageConfig struct {
	observer   StageObserver
	bufferSize int
}

// WithObserver attaches an observer to a stage for monitoring.
func WithObserver(obs StageObserver) StageOption {
	return func(cfg *stageConfig) { cfg.observer = obs }
}

// WithBufferSize sets the output channel buffer size for a stage.
// Default is 0 (unbuffered — full synchronous backpressure).
func WithBufferSize(size int) StageOption {
	return func(cfg *stageConfig) { cfg.bufferSize = size }
}

func applyOpts(opts []StageOption) stageConfig {
	var cfg stageConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
