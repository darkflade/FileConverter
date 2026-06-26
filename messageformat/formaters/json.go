package formaters

import (
	"FileConverter/eventtransport"
	"FileConverter/messageformat"
	"encoding/json"
)

type JSONFormatter struct{}

func (JSONFormatter) Marshal(v any) ([]byte, error) {
	switch msg := v.(type) {
	case eventtransport.Envelope:
		return marshalEnvelope(msg)
	default:
		return json.Marshal(msg)
	}

	return json.Marshal(v)
}

func (JSONFormatter) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (JSONFormatter) Type() messageformat.FormatType {
	return messageformat.JSON
}
