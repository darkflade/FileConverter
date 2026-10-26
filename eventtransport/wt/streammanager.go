package wt

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/quic-go/webtransport-go"
)

type StreamManager struct {
	mu sync.RWMutex

	streams map[uuid.UUID]*DataStream
}

func (m *StreamManager) StreamAppend(stream *DataStream) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.streams[stream.ID]; exists {
		return fmt.Errorf("stream %s already exists", stream.ID)
	}

	m.streams[stream.ID] = stream

	return nil
}

func (m *StreamManager) Get(id uuid.UUID) (*DataStream, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.streams[id]
	return s, ok
}

func (m *StreamManager) Remove(id uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.streams, id)
}

func NewStreamManager() *StreamManager {
	return &StreamManager{
		streams: make(map[uuid.UUID]*DataStream),
	}
}

type DataStream struct {
	ID      uuid.UUID
	Kind    StreamKind
	Stream  *webtransport.Stream
	Created time.Time
}

func NewDataStream(stream *webtransport.Stream, kind StreamKind) (*DataStream, error) {
	ID, err := uuid.NewV7()
	if err != nil {
		fmt.Printf("[ERROR] UUID V7 ERROR: %s\n", err)
		return nil, err
	}

	return &DataStream{
		ID:      ID,
		Kind:    kind,
		Stream:  stream,
		Created: time.Now(),
	}, nil
}

type StreamKind int

const (
	Control StreamKind = iota
	Upload
	Download
	PingPong
)

var streamKindName = map[StreamKind]string{
	Control:  "control",
	Upload:   "upload",
	Download: "download",
	PingPong: "ping-pong",
}

func (sk StreamKind) String() string {
	return streamKindName[sk]
}
