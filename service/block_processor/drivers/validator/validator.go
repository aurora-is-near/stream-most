package validator

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
)

type Validator struct {
	input  chan messages.AbstractNatsMessage
	output chan messages.AbstractNatsMessage
	obs    *observer.Observer

	currentAnnouncement messages.AbstractNatsMessage
	shardsLeft          map[uint8]struct{}
}

func (n *Validator) BindObserver(obs *observer.Observer) {
	n.obs = obs
}

func (n *Validator) Bind(input chan messages.AbstractNatsMessage, output chan messages.AbstractNatsMessage) {
	n.input = input
	n.output = output
}

func (n *Validator) Run() {
	defer close(n.output)
	for msg := range n.input {
		n.process(msg)
	}
}

func (n *Validator) process(message messages.AbstractNatsMessage) {
	if message.IsAnnouncement() {
		n.processAnnouncement(message)
	} else if message.IsShard() {
		n.processShard(message)
	} else {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(message, ErrUnknownMessageType),
		)
	}
}

func (n *Validator) FinishError() error {
	return nil
}

func (n *Validator) processAnnouncement(msg messages.AbstractNatsMessage) {
	if !n.previousCompleted() {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(msg, ErrIncompleteBlock),
		)
	}

	if n.currentAnnouncement != nil &&
		msg.GetBlock().Height <= n.currentAnnouncement.GetBlock().Height {
		n.obs.Emit(observer.ErrorInData,
			observer.WrapMessage(msg, ErrHeightUnordered),
		)
	}

	n.currentAnnouncement = msg
	n.shardsLeft = map[uint8]struct{}{}
	for k, v := range msg.GetAnnouncement().ParticipatingShardsMap {
		if v {
			n.shardsLeft[uint8(k)] = struct{}{}
		}
	}
}

func (n *Validator) processShard(msg messages.AbstractNatsMessage) {
	if n.currentAnnouncement == nil {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(msg, WarnPrecedingShards),
		)
		return
	}

	if msg.GetBlock().Hash != n.currentAnnouncement.GetBlock().Hash {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(msg, ErrPrecedingShards),
		)
		return
	}

	if _, exists := n.shardsLeft[msg.GetShard().ShardID]; !exists {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(msg, ErrUndesiredShard),
		)
		return
	}

	delete(n.shardsLeft, msg.GetShard().ShardID)
}

func (n *Validator) previousCompleted() bool {
	if n.currentAnnouncement == nil {
		return true
	}

	return len(n.shardsLeft) == 0
}

func NewValidator() *Validator {
	return &Validator{}
}
