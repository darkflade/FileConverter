package formaters

import (
	"FileConverter/eventtransport"
	"FileConverter/messageformat"
	"FileConverter/messageformat/protocol"
	"encoding/json"
	"fmt"
)

type JSONFormatter struct{}

func (JSONFormatter) Marshal(v any) ([]byte, error) {
	switch msg := v.(type) {

	case eventtransport.Envelope:
		return marshalEnvelope(msg)

	default:
		return json.Marshal(msg)
	}
}

func (JSONFormatter) Unmarshal(data []byte, v any) error {

	//return json.Unmarshal(data, v)
	var raw map[string]json.RawMessage

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	env := eventtransport.Envelope{}

	if nonce, ok := raw["nonce"]; ok {
		if err := json.Unmarshal(nonce, &env.Nonce); err != nil {
			return err
		}

		delete(raw, "nonce")
	}

	for name, payload := range raw {

		registry := protocol.GetConstructorRegister()
		ctor, ok := registry[name]
		if !ok {
			return fmt.Errorf("unknown message type %q", name)
		}

		msg := ctor()

		if err := json.Unmarshal(payload, msg); err != nil {
			return err
		}

		env.Payload = msg
	}
	
	return nil
}

func (JSONFormatter) Type() messageformat.FormatType {
	return messageformat.JSON
}
