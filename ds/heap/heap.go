package heap

import (
	"fmt"
	"math"
)

type CompareFunc[T any] func(a, b T) bool

type Heap[T any] struct {
	data        []T
	compareFunc CompareFunc[T]
}

func NewEmptyHeap[T any](compareFunc CompareFunc[T]) *Heap[T] {
	return &Heap[T]{
		data:        []T{},
		compareFunc: compareFunc,
	}
}

func NewFromSlice[T any](data []T, compareFunc CompareFunc[T]) *Heap[T] {

	waitHeapify := &Heap[T]{
		data:        data,
		compareFunc: compareFunc,
	}

	if len(waitHeapify.data) > 1 {
		for i := len(data)/2 - 1; i >= 0; i-- {
			waitHeapify.heapify(i)
		}
	}

	return waitHeapify
}

func (h *Heap[T]) Push(value T) {
	h.data = append(h.data, value)
	if len(h.data) > 1 {
		h.shiftUp(len(h.data) - 1)
	}
}

func (h *Heap[T]) Pop() (T, error) {

	if len(h.data) == 0 {
		var zero T
		return zero, fmt.Errorf("empty heap")
	}

	value := h.data[0]
	h.data = h.data[1:]
	if len(h.data) > 0 {
		h.heapify(0)
	}

	return value, nil
}

func (h *Heap[T]) Size() int {
	return len(h.data)
}

func (h *Heap[T]) Empty() bool {
	return len(h.data) == 0
}

func (h *Heap[T]) Clear() {
	h.data = []T{}
}

func (h *Heap[T]) Print() {
	if len(h.data) == 0 {
		fmt.Println("Heap is empty")
		return
	}

	// 计算堆的高度
	height := int(math.Floor(math.Log2(float64(len(h.data)))) + 1)

	// 按层级打印
	fmt.Println("Heap Tree Structure:")
	for level := 0; level < height; level++ {
		// 计算当前层的节点范围
		start := 1<<uint(level) - 1 // 2^level - 1
		end := 1<<uint(level+1) - 1 // 2^(level+1) - 1
		if start >= len(h.data) {
			break
		}

		// 打印层级标题
		fmt.Printf("Level %d: ", level)

		// 打印当前层的所有节点
		for i := start; i < end && i < len(h.data); i++ {
			// 缩进和前缀
			if i == start {
				fmt.Print("|--")
			} else {
				fmt.Print(" |--")
			}
			fmt.Printf("[%d:%v]", i, h.data[i])
		}
		fmt.Println()
	}
}

func (h *Heap[T]) heapify(root int) {
	est := root
	left := root*2 + 1
	right := root*2 + 2

	if left < len(h.data) && h.compareFunc(h.data[left], h.data[root]) {
		est = left
	}

	if right < len(h.data) && h.compareFunc(h.data[right], h.data[0]) {
		est = right
	}

	if est != root {
		h.data[est], h.data[root] = h.data[root], h.data[est]
		h.heapify(est)
	}
}

func (h *Heap[T]) shiftUp(child int) {
	parent := (child - 1) / 2

	if parent < 0 || parent == child || h.compareFunc(h.data[parent], h.data[child]) {
		return
	}

	h.data[child], h.data[parent] = h.data[parent], h.data[child]
	h.shiftUp(parent)
}
