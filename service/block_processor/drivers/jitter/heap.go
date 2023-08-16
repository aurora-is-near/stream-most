package jitter

import (
	"container/heap"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type DelayedMessage struct {
	Message       messages.BlockMessage
	ReturnAtClock uint64
}

type DelayedMessagesHeap struct {
	Messages []DelayedMessage

	heap.Interface
}

func (d *DelayedMessagesHeap) Len() int {
	return len(d.Messages)
}

func (d *DelayedMessagesHeap) Less(i, j int) bool {
	return d.Messages[i].ReturnAtClock < d.Messages[j].ReturnAtClock
}

func (d *DelayedMessagesHeap) Swap(i, j int) {
	d.Messages[i], d.Messages[j] = d.Messages[j], d.Messages[i]
}

func (d *DelayedMessagesHeap) Push(x interface{}) {
	d.Messages = append(d.Messages, x.(DelayedMessage))
}

func (d *DelayedMessagesHeap) Pop() interface{} {
	l := len(d.Messages)
	x := d.Messages[l-1]
	d.Messages = d.Messages[:l-1]
	return x
}
