package formats

var def *Facade

func UseFormat(format FormatType) {
	def = NewFacade()
	def.UseFormat(format)
}

func Active() *Facade {
	if def == nil {
		panic(`You tried to use formats.Active(), but didn't select a format. 
Call formats.UseFormat() first, or instantiate a new facade with formats.NewFacade() yourself and use it`)
	}
	return def
}
