package tests

import "testing"

type A struct {
	a int
}

func (a A) setToOne() {
	a.a = 1
}

func (a *A) setToOnePointer() {
	a.a = 1
}

type B struct {
	a int
}

func (b B) setToOne() {
	b.a = 1
}

func (b *B) setToOnePointer() {
	b.a = 1
}

func TestMethod(t *testing.T) {
	cases := []struct {
		name string
		a    A
		b    *B
	}{
		{
			name: "Receiver Test",
			a:    A{},
			b:    &B{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.a.setToOne()
			if tc.a.a != 0 {
				t.Errorf("Value-Value got %d, want %d", tc.a.a, 0)
			}

			tc.a.setToOnePointer()
			if tc.a.a != 1 {
				t.Errorf("Value-Pointer got %d, want %d", tc.a.a, 1)
			}

			tc.b.setToOne()
			if tc.b.a != 0 {
				t.Errorf("Pointer-Value got %d, want %d", tc.b.a, 0)
			}

			tc.b.setToOnePointer()
			if tc.b.a != 1 {
				t.Errorf("Pointer-Pointer got %d, want %d", tc.b.a, 1)
			}
		})
	}
}
