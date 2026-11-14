package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// newTestPool builds a pool with n proxy exits, all of them usable.
func newTestPool(t *testing.T, n int) *egressPool {
	t.Helper()
	pool := &egressPool{
		mode:      "socks",
		selection: selectionRoundRobin,
		direct:    &egressEntry{kind: "direct", label: "direct"},
		aaaa:      map[string]bool{},
		aaaaHit:   map[string]time.Time{},
	}
	for i := 0; i < n; i++ {
		pool.socks = append(pool.socks, &egressEntry{
			kind:  "socks",
			label: "exit-" + string(rune('a'+i)),
		})
	}
	return pool
}

// Round-robin must hand out a different exit on every call: that is what
// spreads a per-IP free-tier limit over the whole pool.
func TestRoundRobinSelectionWalksThePool(t *testing.T) {
	pool := newTestPool(t, 4)
	seen := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		entry, cooldown := pool.pick("api.example.com", policySocks)
		if entry == nil {
			t.Fatalf("pick %d returned no exit", i)
		}
		if cooldown != 0 {
			t.Fatalf("pick %d reported cooldown %s", i, cooldown)
		}
		seen = append(seen, entry.label)
	}
	unique := map[string]bool{}
	for _, label := range seen {
		unique[label] = true
	}
	if len(unique) != 4 {
		t.Fatalf("round-robin reused exits: %v", seen)
	}
}

// A parked (rate-limited or dead) exit must be skipped, not retried.
func TestRoundRobinSkipsParkedExit(t *testing.T) {
	pool := newTestPool(t, 3)
	parked := pool.socks[0]
	parked.notBefore.Store(time.Now().Add(5 * time.Minute).UnixNano())

	entry, _ := pool.pick("api.example.com", policySocks)
	if entry == nil {
		t.Fatal("pick returned no exit")
	}
	if entry == parked {
		t.Fatalf("pick returned the parked exit %s", parked.label)
	}
}

// When every exit is parked the caller learns how long it has to wait.
func TestRoundRobinReportsCooldownWhenEverythingIsParked(t *testing.T) {
	pool := newTestPool(t, 2)
	for _, entry := range pool.socks {
		entry.notBefore.Store(time.Now().Add(90 * time.Second).UnixNano())
	}
	entry, cooldown := pool.pick("api.example.com", policySocks)
	if entry != nil {
		t.Fatalf("pick returned %s although every exit is parked", entry.label)
	}
	if cooldown <= 0 || cooldown > 90*time.Second {
		t.Fatalf("cooldown = %s, want a positive wait below 90s", cooldown)
	}
}

// The "fastest" strategy keeps preferring the exit with the lowest latency.
func TestFastestSelectionPrefersLowLatency(t *testing.T) {
	pool := newTestPool(t, 3)
	pool.selection = selectionFastest
	pool.socks[0].latencyEWMA.Store(int64(2 * time.Second))
	pool.socks[1].latencyEWMA.Store(int64(100 * time.Millisecond))
	pool.socks[2].latencyEWMA.Store(int64(time.Second))

	for i := 0; i < 5; i++ {
		entry, _ := pool.pick("api.example.com", policySocks)
		if entry == nil {
			t.Fatal("pick returned no exit")
		}
		if entry != pool.socks[1] {
			t.Fatalf("pick returned %s, want the 100ms exit", entry.label)
		}
	}
}

// A transport failure that means "the exit is gone" must park the exit for
// minutes, not for a couple of seconds.
func TestDeadExitIsParkedForMinutes(t *testing.T) {
	entry := &egressEntry{kind: "socks", label: "dead"}
	entry.parkDead("dial tcp: connection refused")
	if entry.usable() {
		t.Fatal("a dead exit must not stay usable")
	}
	remaining := time.Until(time.Unix(0, entry.notBefore.Load()))
	if remaining < deadExitCooldown-time.Minute {
		t.Fatalf("dead exit parked for %s, want about %s", remaining, deadExitCooldown)
	}
}

// The network-level failures that mean "this proxy is gone" have to be
// recognised, while an upstream error must not park the exit.
func TestUnreachableExitDetection(t *testing.T) {
	dead := []error{
		errors.New("dial tcp 10.0.0.1:1080: connect: connection refused"),
		errors.New("dial tcp 10.0.0.1:1080: i/o timeout"),
		errors.New("dial tcp: proxyconnect tcp: connection refused"),
	}
	for _, err := range dead {
		if !unreachableExit(err) {
			t.Fatalf("%v should be treated as an unreachable exit", err)
		}
	}
	if unreachableExit(errors.New("upstream returned 429")) {
		t.Fatal("an upstream error must not park the exit as dead")
	}
	if unreachableExit(nil) {
		t.Fatal("nil is not an unreachable exit")
	}
}

// The default selection is round-robin, so consecutive requests spread over
// the pool instead of piling onto the fastest exit.
func TestDefaultSelectionIsRoundRobin(t *testing.T) {
	cfg, err := decodeConfig([]byte(`{"endpoints":[{"name":"kilo","url":"https://api.kilo.ai/api/gateway/v1"}]}`))
	if err != nil {
		t.Fatalf("decodeConfig: %v", err)
	}
	if cfg.Egress.Selection != selectionRoundRobin {
		t.Fatalf("selection = %q, want %q", cfg.Egress.Selection, selectionRoundRobin)
	}
	if _, err = decodeConfig([]byte(`{"endpoints":[{"name":"a","url":"https://example.com/v1"}],"egress":{"selection":"nope"}}`)); err == nil ||
		!strings.Contains(err.Error(), "egress.selection") {
		t.Fatalf("expected an egress.selection error, got %v", err)
	}
}
