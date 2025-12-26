package fanin

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"
)

type mockObserver struct {
	name string
	t    *testing.T
}

// OnInputAdded is called each time a new input channel is registered via Add().
func (o *mockObserver) OnInputAdded() {
	o.t.Logf("%s: OnInputAdded should work", o.name)
}

// OnInputClosed is called when an input channel is closed by its producer.
func (o *mockObserver) OnInputClosed() {
	o.t.Logf("%s: OnInputClosed should work", o.name)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewFanIn(t *testing.T) {
	t.Parallel()

	obs := new(mockObserver)

	tests := []struct {
		name      string
		cfg       *FanInConfig
		shouldErr bool
	}{
		{
			name: "config with valid field",
			cfg: &FanInConfig{
				BufferSize: 1,
				Observer:   obs,
			},
			shouldErr: false,
		},
		{
			name: "config with invalid buffer size",
			cfg: &FanInConfig{
				BufferSize: -1,
				Observer:   obs,
			},
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewFanIn[int](func(fic *FanInConfig) {
				fic.BufferSize = test.cfg.BufferSize
				fic.Observer = test.cfg.Observer
			})

			if (err != nil) != test.shouldErr {
				t.Errorf("validation error = %v, want err %v", err, test.shouldErr)
			}
		})
	}
}

func TestFanIn_Add(t *testing.T) {
	t.Parallel()
	fi, _ := NewFanIn[int]()

	inputs := make([]chan int, 10)
	for i := range inputs {
		inputs[i] = make(chan int, 1)
		inputs[i] <- i
		close(inputs[i])
	}

	for _, ch := range inputs {
		fi.Add(ch)
	}

}

func TestFanIn_Run(t *testing.T) {
	t.Parallel()
	fi, _ := NewFanIn[int]()

	ch := make(chan int)

	go func() {
		ch <- 1
		ch <- 2
		ch <- 3
		close(ch)
	}()

	fi.Add(ch)

	for v := range fi.Run(t.Context()) {
		t.Logf("value received %d", v)
	}

	<-fi.Done()
}

func TestFanIn_ContextCancellation(t *testing.T) {
	t.Parallel()
	fi, _ := NewFanIn[int]()
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	inputs := make([]chan int, 10)
	for i := range inputs {
		inputs[i] = make(chan int, 1)
		inputs[i] <- i
		close(inputs[i])
	}

	for _, ch := range inputs {
		fi.Add(ch)
	}

	select {
	case <-fi.Run(ctx):
		cancel()
	case <-fi.Done():
		cancel()
	}

}

func TestFanIn_ShouldPanicRunMultiple(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expect panic Run() called multiple times")
		}
	}()

	ctx := t.Context()

	fi, _ := NewFanIn[int]()

	inputs := make([]chan int, 10)
	for i := range inputs {
		inputs[i] = make(chan int, 1)
		inputs[i] <- i
		close(inputs[i])
	}

	for _, ch := range inputs {
		fi.Add(ch)
	}

	for v := range fi.Run(ctx) {
		t.Logf("value received %d", v)
	}

	fi.Run(ctx)
}

func TestFanIn_ShouldPanicAddAfterRun(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expect panic invoked Add() after Run()")
		}
	}()

	fi, _ := NewFanIn[int]()

	inputs := make([]chan int, 10)
	for i := range inputs {
		inputs[i] = make(chan int, 1)
		inputs[i] <- i
		close(inputs[i])
	}

	for _, ch := range inputs {
		fi.Add(ch)
	}

	for v := range fi.Run(t.Context()) {
		t.Logf("value received %d", v)
	}

	fi.Add(make(<-chan int))
}

func TestFanIn_Observer(t *testing.T) {
	t.Parallel()
	obs := &mockObserver{
		t: t,
	}

	fi, _ := NewFanIn[int](
		func(fic *FanInConfig) {
			fic.Observer = obs
		},
	)

	inputs := make([]chan int, 10)
	for i := range inputs {
		inputs[i] = make(chan int, 1)
		inputs[i] <- i
		close(inputs[i])
	}

	for _, ch := range inputs {
		fi.Add(ch)
	}

	for v := range fi.Run(t.Context()) {
		t.Logf("value received %d", v)
	}

	<-fi.Done()
}
