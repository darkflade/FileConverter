package protocol

import (
	"FileConverter/eventtransport"
)

func init() {

	RegisterConstructor("error", func() eventtransport.EventMessagePayload {
		return &eventtransport.EventError{}
	})

	RegisterConstructor("job_created", func() eventtransport.EventMessagePayload {
		return &eventtransport.JobCreated{}
	})

	RegisterConstructor("metadata", func() eventtransport.EventMessagePayload {
		return &eventtransport.Metadata{}
	})
}

var constructorRegistry = map[string]func() eventtransport.EventMessagePayload{}

func GetConstructorRegister() map[string]func() eventtransport.EventMessagePayload {
	return constructorRegistry
}

func RegisterConstructor(name string, ctor func() eventtransport.EventMessagePayload) {
	constructorRegistry[name] = ctor
}
