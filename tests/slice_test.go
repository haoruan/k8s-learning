package tests

import (
	"container/list"
	"testing"
)

func BenchmarkSlice(b *testing.B) {
	l := make([]int, b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		l = l[1:]
	}
	b.StopTimer()

	if len(l) != 0 {
		b.Errorf("unexpected result; got=%v", l)
	}
}

func BenchmarkLists(b *testing.B) {
	l := list.New()
	for n := 0; n < b.N; n++ {
		l.PushBack(0)
	}

	b.ResetTimer()
	for e := l.Front(); e != nil; {
		t := e.Next()
		l.Remove(e)
		e = t
	}

	b.StopTimer()

	if l.Len() != 0 {
		b.Errorf("unexpected result; got=%v", l)
	}
}
