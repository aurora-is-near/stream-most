package streaminput

import "github.com/aurora-is-near/stream-most/service/blockio"

type sequencedMsg struct {
	seq uint64
	msg blockio.Msg
}
