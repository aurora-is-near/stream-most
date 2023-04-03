package metrics

import "github.com/aurora-is-near/stream-most/service/block_processor/observer"

func RegisterObservations(obs *observer.Observer) {
	obs.On(observer.NewAnnouncement, func(data interface{}) {
		AnnouncementsProcessed.Inc()
	})

	obs.On(observer.NewShard, func(data interface{}) {
		ShardsProcessed.Inc()
	})
}
