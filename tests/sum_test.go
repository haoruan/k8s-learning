package tests

import (
	"fmt"
	"runtime"
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
		for i = 0; i < numCpu; i++ {
			go func(i, n int64, stopCh chan<- int64) {
				var sum int64
				for ; i < n; i++ {
					sum += i
				}
				stopCh <- sum
			}(i*n/numCpu, (i+1)*n/numCpu, stopCh)
		}

		for i = 0; i < numCpu; i++ {
			t += <-stopCh
		}
	}
	b.StopTimer()
}
