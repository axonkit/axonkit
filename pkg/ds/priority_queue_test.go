package ds

import (
	"container/heap"
	"testing"
)

func TestPriorityQueue_Basic(t *testing.T) {
	pq := NewPriorityQueue[string](0)

	heap.Push(&pq, &Item[string]{Value: "low", Priority: 10})
	heap.Push(&pq, &Item[string]{Value: "high", Priority: 1})
	heap.Push(&pq, &Item[string]{Value: "mid", Priority: 5})

	if pq.Len() != 3 {
		t.Fatalf("expected 3 items, got %d", pq.Len())
	}

	item := heap.Pop(&pq).(*Item[string])
	if item.Value != "high" || item.Priority != 1 {
		t.Fatalf("expected high/1, got %s/%d", item.Value, item.Priority)
	}

	item = heap.Pop(&pq).(*Item[string])
	if item.Value != "mid" || item.Priority != 5 {
		t.Fatalf("expected mid/5, got %s/%d", item.Value, item.Priority)
	}
}

func TestPriorityQueue_PeekAndShift(t *testing.T) {
	pq := NewPriorityQueue[string](0)

	item, wait := pq.PeekAndShift(100)
	if item != nil || wait != 0 {
		t.Fatal("expected nil for empty queue")
	}

	heap.Push(&pq, &Item[string]{Value: "a", Priority: 10})
	heap.Push(&pq, &Item[string]{Value: "b", Priority: 20})

	item, wait = pq.PeekAndShift(5)
	if item != nil || wait != 5 {
		t.Fatalf("expected nil with wait=5, got item=%v wait=%d", item, wait)
	}

	item, wait = pq.PeekAndShift(10)
	if item == nil || item.Value != "a" || wait != 0 {
		t.Fatal("expected item 'a'")
	}

	item, _ = pq.PeekAndShift(25)
	if item == nil || item.Value != "b" {
		t.Fatal("expected item 'b'")
	}
}

func TestPriorityQueue_GrowShrink(t *testing.T) {
	pq := NewPriorityQueue[int](2)

	for i := 0; i < 100; i++ {
		heap.Push(&pq, &Item[int]{Value: i, Priority: int64(100 - i)})
	}
	if pq.Len() != 100 {
		t.Fatalf("expected 100 items, got %d", pq.Len())
	}

	for pq.Len() > 0 {
		heap.Pop(&pq)
	}
	if pq.Len() != 0 {
		t.Fatal("expected empty queue")
	}
}

func TestPriorityQueue_Swap(t *testing.T) {
	pq := NewPriorityQueue[string](2)
	heap.Push(&pq, &Item[string]{Value: "a", Priority: 1})
	heap.Push(&pq, &Item[string]{Value: "b", Priority: 2})

	pq.Swap(0, 1)
	if pq[0].Value != "b" || pq[1].Value != "a" {
		t.Fatal("swap failed")
	}
	if pq[0].Index != 0 || pq[1].Index != 1 {
		t.Fatal("indices not updated")
	}
}
