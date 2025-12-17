package main

import (
	"context"
	"log"
	"testing"
)

type MockConsumer struct {
	Id      string
	Message string
}

func (c *MockConsumer) Execute(ctx context.Context) error {
	return nil
}

func (c *MockConsumer) GetID() string {
	return c.Id
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    WorkerPoolConfig[*MockConsumer]
		shouldErr bool
	}{
		{
			name: "it should work",
			config: WorkerPoolConfig[*MockConsumer]{
				Name:       "test-config-validate-1",
				NumWorkers: 1,
				TaskSize:   1,
			},
			shouldErr: false,
		},
		{
			name: "it should work",
			config: WorkerPoolConfig[*MockConsumer]{
				Name:       "test-config-validate-2",
				NumWorkers: 0,
				TaskSize:   1,
			},
			shouldErr: true,
		},
		{
			name: "it should work",
			config: WorkerPoolConfig[*MockConsumer]{
				Name:       "test-config-validate-3",
				NumWorkers: 1,
				TaskSize:   0,
			},
			shouldErr: true,
		},
		{
			name: "it should work",
			config: WorkerPoolConfig[*MockConsumer]{
				Name:       "test-config-validate-4",
				NumWorkers: 0,
				TaskSize:   0,
			},
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewWorkerPool[*MockConsumer](t.Context(), &test.config)
			if (err != nil) != test.shouldErr {
				log.Fatalf("Validation error = %v, want err %v", err, test.shouldErr)
			}
		})
	}
}
