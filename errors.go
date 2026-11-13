package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"time"
)

// parkedError means every exit for the host is in cooldown. It carries the
// wait so callers can tell the agent when to retry instead of hanging.
type parkedError struct {
	message string
	wait    time.Duration
}

func (e *parkedError) Error() string {
	return fmt.Sprintf("%s; retryable=true; retry_in=%.0fs", e.message, e.wait.Seconds())
}

func newParkedError(message string, wait time.Duration) error {
	return &parkedError{message: message, wait: wait}
}

func isParkedError(err error) bool {
	var target *parkedError
	return errors.As(err, &target)
}

// retryableNetworkError reports whether a transport error is worth another exit.
func retryableNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if isParkedError(err) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	text := strings.ToLower(err.Error())
	for _, needle := range []string{"timeout", "connection reset", "broken pipe", "eof", "no such host", "temporary failure", "no suitable address"} {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

// retryableStatus reports whether an upstream status is worth another try.
func retryableStatus(status int) bool {
	switch status {
	case 408, 409, 425, 429, 500, 502, 503, 504, 522, 523, 524, 529:
		return true
	}
	return false
}

// rateLimitedStatus reports statuses that mean "this exit is throttled".
func rateLimitedStatus(status int) bool {
	return status == 429 || status == 503 || status == 529
}

// failureReport renders one attempt failure for logs and for the client error.
type failureReport struct {
	attempts []string
}

func (f *failureReport) add(label string, err error) {
	if f == nil {
		return
	}
	f.attempts = append(f.attempts, label+": "+err.Error())
}

func (f *failureReport) String() string {
	if f == nil || len(f.attempts) == 0 {
		return ""
	}
	return strings.Join(f.attempts, " | ")
}
