package stream

import "github.com/nats-io/nats.go"

func RawMsgToMsg(msg *nats.RawStreamMsg) (*nats.Msg, error) {
	m := &nats.Msg{
		Subject: msg.Subject,
		Header:  msg.Header,
		Data:    msg.Data,
	}
	m.Metadata()
	return m, nil
}
