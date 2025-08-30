package heap

import (
	"fmt"
	"testing"
)

func TestHeap(t *testing.T) {

	littleHeap := NewEmptyHeap[int](func(a, b int) bool {
		return a < b
	})

	littleHeap.Push(2)
	littleHeap.Push(8)
	littleHeap.Push(8)
	littleHeap.Push(3)
	littleHeap.Push(9)
	littleHeap.Push(1)
	littleHeap.Push(2)

	littleHeap.Print()

	for i := 0; i < 7; i++ {
		value, err := littleHeap.Pop()
		if err != nil {
			t.Error(err)
		}
		fmt.Println(value)
	}

}

func TestOrderedHeap(t *testing.T) {

	littleHeap := NewOrderedBigHeap[int]()

	littleHeap.Push(2)
	littleHeap.Push(8)
	littleHeap.Push(8)
	littleHeap.Push(3)
	littleHeap.Push(9)
	littleHeap.Push(1)

	for i := 0; i < 6; i++ {
		value, err := littleHeap.Pop()
		if err != nil {
			t.Error(err)
		}
		fmt.Println(value)
	}

}
