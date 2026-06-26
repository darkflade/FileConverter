package formaters

import (
	"FileConverter/messageformat"
	"fmt"
)

func New(format messageformat.FormatType) (messageformat.Formatter, error) {
	switch format {
	case messageformat.JSON:
		return JSONFormatter{}, nil
	case messageformat.MessagePack:
		return MessagePackFormatter{}, nil
	case messageformat.CBOR:
		return CBORFormatter{}, nil
	default:
		return nil, fmt.Errorf("formatter %s is not implemented", format)
	}
}
