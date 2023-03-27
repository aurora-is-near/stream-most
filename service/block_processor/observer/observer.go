package observer

type EventType string

var (
	NewAnnouncement EventType = "new_announcement"
	NewShard        EventType = "new_shard"
	RescueNeeded    EventType = "rescue_needed"
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
