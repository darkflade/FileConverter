package messageformat

import (
	"fmt"
	"strings"
)

type FormatType int

const (
	JSON FormatType = iota
	MessagePack
	Protobuf
	CBOR
	//	FlatBuffers
)

var formatNames = map[FormatType]string{
	JSON:        "JSON",
	MessagePack: "MessagePack",
	Protobuf:    "Protobuf",
	CBOR:        "CBOR",
	//	FlatBuffers: "FlatBuffers",
}

func (ft FormatType) String() string {
	return formatNames[ft]
}

type Formatter interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Type() FormatType
}

func ParseFormat(value string) (FormatType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "json":
		return JSON, nil
	case "messagepack", "msgpack", "mpack":
		return MessagePack, nil
	case "cbor":
		return CBOR, nil
	case "protobuf", "proto":
		return Protobuf, fmt.Errorf("protobuf requires a shared .proto schema and is not wired yet")
	case "flatbuffers", "flatbuffer":
		return JSON, fmt.Errorf("flatbuffers requires a shared .fbs schema and is not wired yet")
	default:
		return JSON, fmt.Errorf("unsupported protocol %q", value)
	}
}
