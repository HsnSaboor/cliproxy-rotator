package main

import (
	"testing"
	"time"
)

func TestWarmRingDefaults(t *testing.T) {
	fast := &egressPool{selection: selectionFastest}
	fast.configureWarmRing(egressConfig{})
	if fast.warmSize != defaultWarmSize {
		t.Fatalf("fastest ring size = %d, want %d", fast.warmSize, defaultWarmSize)
	}
	rr := &egressPool{selection: selectionRoundRobin}
	rr.configureWarmRing(egressConfig{})
	if rr.warmSize <= defaultWarmSize {
		t.Fatalf("round-robin ring size = %d, want wider than %d", rr.warmSize, defaultWarmSize)
	}
	off := &egressPool{}
	off.configureWarmRing(egressConfig{WarmSize: -1})
	if off.warmSize != 0 {
		t.Fatalf("negative warm_pool_size should disable the ring, got %d", off.warmSize)
	}
}

func TestWarmRingNextSkipsParkedAndSaturated(t *testing.T) {
	ok := &egressEntry{label: "ok"}
	parked := &egressEntry{label: "parked"}
	parked.notBefore.Store(time.Now().Add(time.Hour).UnixNano())
	ring := &warmRing{entries: []*egressEntry{parked, ok}, built: time.Now()}
	for i := 0; i < 4; i++ {
		if got := ring.next(); got != ok {
			t.Fatalf("next() = %v, want the healthy exit", got)
		}
	}
	ring.entries = []*egressEntry{parked}
	if got := ring.next(); got != nil {
		t.Fatalf("next() = %v with everything parked, want nil", got)
	}
}

func TestWarmRingStale(t *testing.T) {
	entry := &egressEntry{label: "e"}
	ring := &warmRing{entries: []*egressEntry{entry}, built: time.Now()}
	if ring.stale(time.Minute) {
		t.Fatal("fresh ring reported stale")
	}
	ring.built = time.Now().Add(-2 * time.Minute)
	if !ring.stale(time.Minute) {
		t.Fatal("old ring not reported stale")
	}
	ring.built = time.Now()
	entry.notBefore.Store(time.Now().Add(time.Minute).UnixNano())
	if !ring.stale(time.Minute) {
		t.Fatal("ring with a parked exit must rebuild")
	}
}

func TestWarmTarget(t *testing.T) {
	got := warmTarget([]endpointConfig{{URL: "https://api.kilo.ai/api/gateway/v1"}})
	if got != "https://api.kilo.ai/api/gateway/v1/models" {
		t.Fatalf("warmTarget = %q", got)
	}
	if warmTarget(nil) != "" {
		t.Fatal("warmTarget without endpoints must be empty")
	}
}

func TestRateLimitCooldown(t *testing.T) {
	if rateLimitCooldown(nil) != 0 {
		t.Fatal("nil entry must have no cooldown")
	}
	entry := &egressEntry{}
	entry.notBefore.Store(time.Now().Add(30 * time.Second).UnixNano())
	if got := rateLimitCooldown(entry); got < 25*time.Second || got > 31*time.Second {
		t.Fatalf("rateLimitCooldown = %v, want about 30s", got)
	}
}
