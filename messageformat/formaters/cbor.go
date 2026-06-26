package formaters

import (
	"FileConverter/messageformat"

	"github.com/fxamacker/cbor/v2"
)

type CBORFormatter struct{}

func (CBORFormatter) Marshal(v any) ([]byte, error) {
	return cbor.Marshal(v)
}

func (CBORFormatter) Unmarshal(data []byte, v any) error {
	return cbor.Unmarshal(data, v)
}

func (CBORFormatter) Type() messageformat.FormatType {
	return messageformat.CBOR
}
