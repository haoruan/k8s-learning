package tests

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

const N = 100000000

func BenchmarkSum(b *testing.B) {
	numCpu := int64(runtime.NumCPU())
	var t, i, n int64 = 0, 0, N / numCpu * numCpu

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		t = 0
		for i = 0; i < n; i++ {
			t += i
		}
	}
	b.StopTimer()

	fmt.Println(t)
}

func BenchmarkParallelSum(b *testing.B) {
	numCpu := int64(runtime.NumCPU())
	stopCh := make(chan int64)
	var i, t, n int64 = 0, 0, N / numCpu * numCpu

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		t = 0
		ch := make(chan int64, numCpu)
		for i := int64(0); i < numCpu; i++ {
			ch <- i
		}
		close(ch)
		for i = 0; i < numCpu; i++ {
			go func() {
				for c := range ch {
					start := c * n / numCpu
					end := (c + 1) * n / numCpu
					var sum int64
					for j := start; j < end; j++ {
						sum += j
					}
					stopCh <- sum
				}
			}()
		}

		for i = 0; i < numCpu; i++ {
			t += <-stopCh
		}
	}
	b.StopTimer()

}

func TestParallel(t *testing.T) {
	ch := make(chan int, 5)
	for i := 0; i < 5; i++ {
		ch <- i
	}
	close(ch)

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			for c := range ch {
				t.Log(c)
			}
		}()
	}
	wg.Wait()
}
