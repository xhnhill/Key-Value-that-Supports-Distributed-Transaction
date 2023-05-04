package test

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"container/heap"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

// Define a slice of Items that implements the heap.Interface
type PriorityQueue []*pb.Message

func (pq PriorityQueue) Len() int {
	return len(pq)
}
func CpTimestamp(t1 *pb.TransTimestamp, t2 *pb.TransTimestamp) bool {
	tp1 := t1.TimeStamp.AsTime()
	tp2 := t2.TimeStamp.AsTime()
	if tp1.Equal(tp2) {
		if t1.Seq == t2.Seq {
			return t1.Id > t2.Id
		} else {
			return t1.Seq > t2.Seq
		}
	} else {
		return tp1.After(tp2)
	}
}
func (pq PriorityQueue) Less(i, j int) bool {
	// Use the custom comparison function to determine which item has higher priority
	//return pq[i].priority < pq[j].priority
	return CpTimestamp(pq[j].T0, pq[i].T0)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	//pq[i].index = i
	//pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*pb.Message)

	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1] // for safety
	*pq = old[0 : n-1]
	return item
}
func generateTimestamp() *pb.TransTimestamp {
	timeNow := time.Now()
	t0 := &pb.TransTimestamp{
		TimeStamp: &timestamppb.Timestamp{
			Seconds: timeNow.Unix(),
			Nanos:   int32(timeNow.Nanosecond()),
		},
		Seq: 0,
		Id:  0,
	}
	return t0
}
func TestReorder(t *testing.T) {
	// Create a new priority queue
	pq := make(PriorityQueue, 0)
	a := generateTimestamp()
	b := generateTimestamp()
	// Push some items onto the priority queue
	heap.Push(&pq, &pb.Message{T0: b})
	heap.Push(&pq, &pb.Message{T0: a})
	heap.Push(&pq, &pb.Message{T0: generateTimestamp()})
	heap.Push(&pq, &pb.Message{T0: generateTimestamp()})
	heap.Push(&pq, &pb.Message{T0: generateTimestamp()})

	// Pop items off the priority queue in order of highest priority
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*pb.Message)
		fmt.Printf("%s", item.T0.TimeStamp.AsTime().String())
	}
}
