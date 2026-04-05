package fanin

import (
	"testing"
)

func BenchmarkFanIn(b *testing.B) {
	b.ReportAllocs()

	fi, _ := NewFanIn[int](WithBufferSize(64))

	producers := 4
	chs := make([]chan int, producers)
	for i := range chs {
		chs[i] = make(chan int, 64)
		fi.Add(chs[i])
	}

	out := fi.Run(b.Context())

	// Drain output
	go func() {
		for range out {
		}
	}()

	for b.Loop() {
		chs[b.N%producers] <- 1
	}

	for _, ch := range chs {
		close(ch)
	}

	<-fi.Done()
}
