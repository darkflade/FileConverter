package eventtransport

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type EventTransport interface {
	Name() string
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
	ReadMessage() ([]byte, error)
	WriteMessage([]byte) error
	WritePing() error
	Close() error
}

type Client struct {
	ID           uuid.UUID
	Transport    EventTransport
	Send         chan []byte
	lastMessage  time.Time
	messageCount int
	rateLimiter  *RateLimiter
	requestNonce string
	requestMu    sync.RWMutex
	closed       bool
	closeMu      sync.Mutex
}

func NewClient(transport EventTransport) *Client {
	ID, err := uuid.NewV7()
	if err != nil {
		return nil
	}
	return &Client{
		ID:          ID,
		Transport:   transport,
		Send:        make(chan []byte, 256),
		lastMessage: time.Now(),
		rateLimiter: NewRateLimiter(600, time.Minute),
	}
}

func (c *Client) Close() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	_ = c.Transport.Close()
	close(c.Send)
}

func (c *Client) IsClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed
}

func (c *Client) SendMessage(payload interface{}) error {
	if c.IsClosed() {
		return NewError("client_closed", "Client connection is closed")
	}

	message := Envelope{
		Nonce:   c.RequestNonce(),
		Payload: payload,
	}

	msgData, err := c.Transport.Marshal(message)

	if err != nil {
		return NewError("client_error", "Failed to marshal envelope")
	}

	select {
	case c.Send <- msgData:
		return nil
	default:

		return NewError("channel_full", "Client message channel is full")
	}
}

func (c *Client) SetRequestNonce(nonce string) {
	c.requestMu.Lock()
	defer c.requestMu.Unlock()
	c.requestNonce = nonce
}

func (c *Client) ClearRequestNonce() {
	c.SetRequestNonce("")
}

func (c *Client) RequestNonce() string {
	c.requestMu.RLock()
	defer c.requestMu.RUnlock()
	return c.requestNonce
}
