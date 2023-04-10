package monitoring

import "github.com/aurora-is-near/stream-most/service/block_processor/observer"

func RegisterObservations(obs *observer.Observer) {
	obs.On(observer.NewAnnouncement, func(interface{}) {
		AnnouncementsProcessed.Inc()
	})

	obs.On(observer.NewShard, func(interface{}) {
		ShardsProcessed.Inc()
	})

	obs.On(observer.ValidationOK, func(interface{}) {
		ValidationPassed.Inc()
	})

	obs.On(observer.ErrorInData, func(interface{}) {
		ValidationError.Inc()
	})
}
