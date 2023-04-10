package observer

type EventType string

var (
	NewAnnouncement            EventType = "new_announcement"
	NewShard                   EventType = "new_shard"
	RescueNeeded               EventType = "rescue_needed"
	BlockAnnouncementDuplicate EventType = "block_announcement_duplicate"

	// ErrorInData indicates that some data on the input stream is corrupted.
	// Data on the callback is driver-specific
	ErrorInData EventType = "error_in_data"

	// ValidationOK happens when validator passes some message as good one
	ValidationOK EventType = "validation_ok"
)

type Observer struct {
	listeners map[EventType][]func(interface{})
}

func (o *Observer) On(event EventType, callback func(interface{})) {
	o.listeners[event] = append(o.listeners[event], callback)
}

func (o *Observer) Emit(event EventType, data interface{}) {
	for _, listener := range o.listeners[event] {
		listener(data)
	}
}

func NewObserver() *Observer {
	return &Observer{
		listeners: make(map[EventType][]func(interface{})),
	}
}
