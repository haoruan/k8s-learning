package tests

import (
	"fmt"
	"testing"
)

var s = "Hello,世界"

func BenchmarkByte(b *testing.B) {
	n := len(s)
	var t byte

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			t = s[j]
		}
	}
	b.StopTimer()

	fmt.Println(t)
}

func BenchmarkRune(b *testing.B) {
	var t rune

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, ch := range s {
			t = ch
		}
	}
	b.StopTimer()

	fmt.Println(t)
}
