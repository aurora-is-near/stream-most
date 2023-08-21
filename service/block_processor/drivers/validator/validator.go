package validator

import (
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
)

type Validator struct {
	input  chan *messages.BlockMessage
	output chan *messages.BlockMessage
	obs    *observer.Observer

	currentAnnouncement *messages.BlockMessage
	shardsLeft          map[uint8]struct{}

	killed bool
}

func (n *Validator) BindObserver(obs *observer.Observer) {
	n.obs = obs
}

func (n *Validator) Bind(input chan *messages.BlockMessage, output chan *messages.BlockMessage) {
	n.input = input
	n.output = output
}

func (n *Validator) Run() {
	defer close(n.output)
	for msg := range n.input {
		if n.killed {
			break
		}
		n.process(msg)
	}
}

func (n *Validator) process(message *messages.BlockMessage) {
	switch message.Block.GetBlockType() {
	case blocks.Announcement:
		n.processAnnouncement(message)
	case blocks.Shard:
		n.processShard(message)
	default:
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(message, ErrUnknownMessageType),
		)
	}
}

func (n *Validator) FinishError() error {
	return nil
}

func (n *Validator) Kill() {
	n.killed = true
}

func (n *Validator) processAnnouncement(msg *messages.BlockMessage) {
	if !n.previousCompleted() {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(msg, ErrIncompleteBlock),
		)
	}

	if n.currentAnnouncement != nil &&
		msg.Block.GetHeight() <= n.currentAnnouncement.Block.GetHeight() {
		n.obs.Emit(observer.ErrorInData,
			observer.WrapMessage(msg, ErrHeightUnordered),
		)
	}

	n.currentAnnouncement = msg
	n.shardsLeft = map[uint8]struct{}{}
	for k, v := range msg.Block.GetShardMask() {
		if v {
			n.shardsLeft[uint8(k)] = struct{}{}
		}
	}
	n.obs.Emit(observer.ValidationOK, observer.WrapMessage(msg, nil))
}

func (n *Validator) processShard(msg *messages.BlockMessage) {
	if n.currentAnnouncement == nil {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(msg, WarnPrecedingShards),
		)
		return
	}

	if msg.Block.GetHash() != n.currentAnnouncement.Block.GetHash() {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(msg, ErrPrecedingShards),
		)
		return
	}

	if _, exists := n.shardsLeft[uint8(msg.Block.GetShardID())]; !exists {
		n.obs.Emit(
			observer.ErrorInData,
			observer.WrapMessage(msg, ErrUndesiredShard),
		)
		return
	}

	delete(n.shardsLeft, uint8(msg.Block.GetShardID()))
	n.obs.Emit(observer.ValidationOK, observer.WrapMessage(msg, nil))
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
