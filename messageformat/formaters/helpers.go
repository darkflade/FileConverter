package formaters

import (
	"FileConverter/eventtransport"
	"FileConverter/messageformat/protocol"
	"encoding/json"
	"fmt"
	"reflect"
)

func marshalEnvelope(e eventtransport.Envelope) ([]byte, error) {

	protocolRegister := protocol.GetTypeRegister()
	name, ok := protocolRegister[reflect.TypeOf(e.Payload)]
	if !ok {
		return nil, fmt.Errorf("unknown envelope payload type: %T", e.Payload)
	}

	obj := map[string]any{
		"nonce": e.Nonce,
		name:    e.Payload,
	}

	return json.Marshal(obj)
}
