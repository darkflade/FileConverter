package formaters

import (
	"FileConverter/eventtransport"
	"encoding/json"
)

func marshalEnvelope(e eventtransport.Envelope) ([]byte, error) {

	data := map[string]any{
		"nonce":                 e.Nonce,
		e.Payload.MessageName(): e.Payload,
	}

	return json.Marshal(data)
}
