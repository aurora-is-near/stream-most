package jitter

import "github.com/aurora-is-near/stream-most/domain/messages"

import (
	"container/heap"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"math/rand"
)

type Jitter struct {
	input    chan messages.AbstractNatsMessage
	output   chan messages.AbstractNatsMessage
	observer *observer.Observer

	delayedMessages *DelayedMessagesHeap
	clock           uint64
	opts            *Options
}

func (j *Jitter) BindObserver(obs *observer.Observer) {
	j.observer = obs
}

func (j *Jitter) Bind(input chan messages.AbstractNatsMessage, output chan messages.AbstractNatsMessage) {
	j.input = input
	j.output = output
}

func (j *Jitter) Run() {
	for msg := range j.input {
		j.clock += 1

		if j.shouldDropout(msg) {
			continue
		}

		if should, delay := j.shouldDelay(msg); should {
			heap.Push(j.delayedMessages, DelayedMessage{
				Message:       msg,
				ReturnAtClock: j.clock + delay,
			})
		} else {
			j.output <- msg
		}

		j.popReadyMessages()
	}
}

func (j *Jitter) shouldDropout(_ messages.AbstractNatsMessage) bool {
	return rand.Float64() < j.opts.DropoutChance
}

func (j *Jitter) shouldDelay(_ messages.AbstractNatsMessage) (bool, uint64) {
	should := rand.Float64() < j.opts.DelayChance
	if !should {
		return false, 0
	}

	delay := uint64(rand.Float64()*float64(j.opts.MaxDelay-j.opts.MinDelay)) + j.opts.MinDelay
	return true, delay
}

func (j *Jitter) popReadyMessages() {
	for j.delayedMessages.Len() > 0 {
		msg := heap.Pop(j.delayedMessages).(DelayedMessage)
		if msg.ReturnAtClock > j.clock {
			heap.Push(j.delayedMessages, msg)
			break
		}

		j.output <- msg.Message
	}
}

func NewJitter(options *Options) *Jitter {
	return &Jitter{
		opts:            options,
		delayedMessages: &DelayedMessagesHeap{},
	}
}
