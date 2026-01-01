package fanout

import (
	"fmt"
	"sync"
	"testing"
)

func TestFanOut_Run(t *testing.T) {
	var wg sync.WaitGroup
	fo, _ := NewFanOut[string](func(foc *FanOutConfig) {
		foc.WorkerSize = 10
		foc.Strategy = BroadCast
	})

	ch := make(chan string, 10)
	wg.Go(func() {
		for i := range 10 {
			ch <- fmt.Sprintf("Hello %d", i)
		}
		close(ch)
	})

	fo.Run(t.Context(), ch)

	for _, cha := range fo.Outputs() {
		wg.Go(func() {
			for v := range cha {
				fmt.Println(v)
			}
		})
	}

	wg.Wait()

}
