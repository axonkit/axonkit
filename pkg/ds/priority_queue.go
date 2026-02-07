package ds

import "container/heap"

// Item is a generic container for priority queue items
type Item[T any] struct {
	Value    T
	Priority int64
	Index    int
}

// PriorityQueue implements a generic min-heap
type PriorityQueue[T any] []*Item[T]

func NewPriorityQueue[T any](capacity int) PriorityQueue[T] {
	return make(PriorityQueue[T], 0, capacity)
}

func (pq PriorityQueue[T]) Len() int {
	return len(pq)
}

func (pq PriorityQueue[T]) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue[T]) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(PriorityQueue[T], n, c*2+1)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*Item[T])
	item.Index = n
	(*pq)[n] = item
}

func (pq *PriorityQueue[T]) Pop() interface{} {
	old := *pq
	n := len(old)
	c := cap(old)

	// Shrink if needed (when usage is less than 1/4 of capacity)
	// We use 1/4 to avoid thrashing around 1/2 mark
	if n < (c/4) && c > 25 {
		npq := make(PriorityQueue[T], n, c/2)
		copy(npq, old)
		*pq = npq
		old = npq
	}

	item := old[n-1]
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

// PeekAndShift returns the top item if its priority is <= max.
// If top item > max, returns nil and the difference (wait time).
func (pq *PriorityQueue[T]) PeekAndShift(max int64) (*Item[T], int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	item := (*pq)[0]
	if item.Priority > max {
		return nil, item.Priority - max
	}
	heap.Remove(pq, 0)

	return item, 0
}
