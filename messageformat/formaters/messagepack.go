package formaters

import (
	"FileConverter/messageformat"

	"github.com/vmihailenco/msgpack/v5"
)

type MessagePackFormatter struct{}

func (MessagePackFormatter) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (MessagePackFormatter) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

func (MessagePackFormatter) Type() messageformat.FormatType {
	return messageformat.MessagePack
}
