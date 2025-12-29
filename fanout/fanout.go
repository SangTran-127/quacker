package fanout

import "fmt"

type FanOutConfig struct {
	numWorkers int
}

type Option func(*FanOutConfig)

type FanOut[T any] struct {
	numWorkers int
}

func NewFanOut[T any](opts ...Option) (*FanOut[T], error) {
	cfg := &FanOutConfig{
		numWorkers: 1,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.numWorkers < 1 {
		return nil, fmt.Errorf("fanout: number of workers must greater than 1")
	}

	return &FanOut[T]{
		numWorkers: cfg.numWorkers,
	}, nil
}
