package main

import (
	"sync"
	"time"
)

// health tracks per-exit and per-endpoint failures so the rotator stops
// hammering dead exits and every request finishes fast.
//
// An exit that fails is parked for a short, growing cooldown (2s → 4s → … up
// to 2 min). A success clears its history. Nothing here blocks a request:
// selection only reads the current snapshot.
type health struct {
	mu        sync.Mutex
	failures  map[string]int
	notBefore map[string]time.Time
	lastError map[string]string
}

func newHealth() *health {
	return &health{
		failures:  make(map[string]int),
		notBefore: make(map[string]time.Time),
		lastError: make(map[string]string),
	}
}

// usable reports whether key may be tried right now.
func (h *health) usable(key string) bool {
	if h == nil {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	until, parked := h.notBefore[key]
	return !parked || time.Now().After(until)
}

// cooldownRemaining reports how long key stays parked (0 when usable).
func (h *health) cooldownRemaining(key string) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if until, parked := h.notBefore[key]; parked {
		if remaining := time.Until(until); remaining > 0 {
			return remaining
		}
	}
	return 0
}

// succeed clears the failure history for key.
func (h *health) succeed(key string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.failures, key)
	delete(h.notBefore, key)
	delete(h.lastError, key)
}

// fail records a failure for key and parks it with exponential backoff.
func (h *health) fail(key string, reason string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failures[key]++
	wait := time.Duration(1<<uint(minInt(h.failures[key], 7))) * time.Second // 2s..128s
	if wait > 2*time.Minute {
		wait = 2 * time.Minute
	}
	h.notBefore[key] = time.Now().Add(wait)
	h.lastError[key] = reason
}

// lastFailureFor returns the recorded reason, if any.
func (h *health) lastFailureFor(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastError[key]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
