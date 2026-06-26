package eventtransport

import (
	"sync"
	"time"
)

type RateLimiter struct {
	maxMessages int
	window      time.Duration
	messages    []time.Time
	mu          sync.Mutex
}

func NewRateLimiter(maxMessages int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		maxMessages: maxMessages,
		window:      window,
		messages:    make([]time.Time, 0, maxMessages),
	}
}

func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rl.window)

	// Remove old messages
	validMessages := make([]time.Time, 0, len(rl.messages))
	for _, msgTime := range rl.messages {
		if msgTime.After(cutoff) {
			validMessages = append(validMessages, msgTime)
		}
	}
	rl.messages = validMessages

	// Check if we can send
	if len(rl.messages) >= rl.maxMessages {
		return false
	}

	// Add current message
	rl.messages = append(rl.messages, now)
	return true
}

func (c *Client) CheckRateLimit() bool {
	return c.rateLimiter.Allow()
}
