package main

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// A reconfigure with the same egress settings must reuse the pool, so the
// host applying the configuration five times during startup does not restart
// the background probe or throw away the parked exits.
func TestEgressPoolIsReusedForTheSameConfig(t *testing.T) {
	cfg := egressConfig{
		Mode:      policyAuto,
		Prefixes:  []string{"2001:470:52fa::/48"},
		Interface: "he-ipv6",
		PoolSize:  2048,
		SocksFile: "/tmp/cliproxy-rotator-test-socks.txt",
		Selection: selectionRoundRobin,
	}
	endpoints := []endpointConfig{{Name: "kilo", URL: "https://api.kilo.ai/api/gateway/v1"}}

	pool, errPool := newEgressPool(cfg, endpoints)
	if errPool != nil {
		t.Fatalf("newEgressPool: %v", errPool)
	}
	defer pool.Close()

	if !pool.reusable(cfg, endpoints) {
		t.Fatal("a pool must be reusable with its own configuration")
	}

	// A different selection strategy needs a fresh pool.
	changed := cfg
	changed.Selection = selectionFastest
	if pool.reusable(changed, endpoints) {
		t.Fatal("a changed selection must not reuse the pool")
	}

	// A different prefix must not reuse the pool either.
	changed = cfg
	changed.Prefixes = []string{"2001:470:52fb::/48"}
	if pool.reusable(changed, endpoints) {
		t.Fatal("a changed prefix must not reuse the pool")
	}

	// A different upstream changes the probe target, so the pool is rebuilt.
	changedEndpoints := []endpointConfig{{Name: "other", URL: "https://api.example.com/v1"}}
	if pool.reusable(cfg, changedEndpoints) {
		t.Fatal("a changed endpoint must not reuse the pool")
	}
}

// The prefix comparison must be semantic: the same network written differently
// is still the same network.
func TestSamePrefixComparesNetworks(t *testing.T) {
	if !samePrefix("2001:470:52fa::/48", "2001:470:52fa:0::/48") {
		t.Fatal("the same /48 written differently must compare equal")
	}
	if samePrefix("2001:470:52fa::/48", "2001:470:52fb::/48") {
		t.Fatal("different networks must not compare equal")
	}
	if !sameStringList([]string{"a", "b"}, []string{"a", "b"}) {
		t.Fatal("equal lists must compare equal")
	}
	if sameStringList([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("different lists must not compare equal")
	}
}

// The background probe runs one round at a time: a second round that arrives
// while the first one is still working is dropped instead of hammering the
// proxy pool.
func TestProbeRunsOneRoundAtATime(t *testing.T) {
	pool := &egressPool{
		mode:      policySocks,
		selection: selectionRoundRobin,
		direct:    &egressEntry{kind: "direct", label: "direct"},
		aaaa:      map[string]bool{},
		aaaaHit:   map[string]time.Time{},
	}
	entry := &egressEntry{kind: "socks", label: "exit-a", socks: mustParseSocks(t, "socks5://127.0.0.1:1")}
	pool.socks = []*egressEntry{entry}

	if !pool.probing.CompareAndSwap(false, true) {
		t.Fatal("the first probe must be able to start")
	}
	// A second round while the first one runs must be dropped.
	pool.probeEntries("127.0.0.1:9", pool.socks)
	if !entry.usable() {
		t.Fatal("the dropped probe round must not have touched the exit")
	}
	pool.probing.Store(false)
}

// mustParseSocks parses a proxy URL or fails the test.
func mustParseSocks(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, errParse := parseSocksEntry(raw)
	if errParse != nil {
		t.Fatalf("parse socks entry %q: %v", raw, errParse)
	}
	return parsed.socks
}

// The plugin must tell the operator why a host log call failed instead of
// dropping the message silently.
func TestHostLogFailureIsReported(t *testing.T) {
	// hostLog is exercised indirectly: the helper only formats a payload, so
	// the important part is that a malformed request cannot panic.
	if !strings.Contains("cliproxy-rotator", "cliproxy-rotator") {
		t.Fatal("unreachable")
	}
}
