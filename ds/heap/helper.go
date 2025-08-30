package heap

import "golang.org/x/exp/constraints"

type OrderedHeap[T constraints.Ordered] struct {
	Heap[T]
}

func NewOrderedLittleHeap[T constraints.Ordered]() *Heap[T] {
	return &Heap[T]{
		data: []T{},
		compareFunc: func(a, b T) bool {
			return a < b
		},
	}
}

func NewOrderedBigHeap[T constraints.Ordered]() *Heap[T] {
	return &Heap[T]{
		data: []T{},
		compareFunc: func(a, b T) bool {
			return a > b
		},
	}
}
