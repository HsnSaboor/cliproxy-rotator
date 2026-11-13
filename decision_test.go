package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDecodeConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := decodeConfig([]byte(`{"provider":"rotator","endpoints":[{"name":"kilo","url":"https://api.kilo.ai/api/gateway/v1","api_keys":[""]}],"models":[{"id":"rotator/particle-1-free","upstream":"particle-ai/particle-1:free"}]}`))
	if err != nil {
		t.Fatalf("decodeConfig: %v", err)
	}
	if cfg.Provider != "rotator" {
		t.Fatalf("provider = %q", cfg.Provider)
	}
	if cfg.MaxAttempts == 0 || cfg.TotalTimeoutSeconds == 0 {
		t.Fatalf("expected time budgets to be defaulted, got %+v", cfg)
	}
	if cfg.Egress.Mode != "auto" {
		t.Fatalf("egress mode = %q, want auto", cfg.Egress.Mode)
	}
	if len(cfg.Models) != 1 || cfg.Models[0].Upstream != "particle-ai/particle-1:free" {
		t.Fatalf("models = %+v", cfg.Models)
	}
}

func TestDecodeConfigRejectsBadEgressMode(t *testing.T) {
	_, err := decodeConfig([]byte(`{"endpoints":[{"name":"a","url":"https://example.com/v1"}],"egress":{"mode":"carrier-pigeon"}}`))
	if err == nil || !strings.Contains(err.Error(), "egress.mode") {
		t.Fatalf("expected an egress.mode error, got %v", err)
	}
}

func TestDecodeConfigRejectsBadEndpointPolicy(t *testing.T) {
	_, err := decodeConfig([]byte(`{"endpoints":[{"name":"a","url":"https://example.com/v1","egress":"nope"}]}`))
	if err == nil || !strings.Contains(err.Error(), "endpoints[0].egress") {
		t.Fatalf("expected an endpoint egress error, got %v", err)
	}
}

func TestDecodeConfigRejectsEmptyEndpoints(t *testing.T) {
	if _, err := decodeConfig([]byte(`{"provider":"rotator","endpoints":[]}`)); err == nil {
		t.Fatal("expected an error for a config with no endpoints")
	}
}

func TestRetryableStatusCoversQuotaAndGatewayErrors(t *testing.T) {
	for _, status := range []int{408, 425, 429, 500, 502, 503, 504, 529} {
		if !retryableStatus(status) {
			t.Fatalf("status %d should be retryable", status)
		}
	}
	for _, status := range []int{400, 401, 403, 404, 422} {
		if retryableStatus(status) {
			t.Fatalf("status %d should not be retryable", status)
		}
	}
}

func TestRateLimitedStatus(t *testing.T) {
	for _, status := range []int{429, 503, 529} {
		if !rateLimitedStatus(status) {
			t.Fatalf("status %d should park the exit", status)
		}
	}
	if rateLimitedStatus(500) {
		t.Fatal("500 is a retry, not a park")
	}
}

func TestRetryableNetworkError(t *testing.T) {
	if retryableNetworkError(nil) {
		t.Fatal("nil is not retryable")
	}
	reset := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	if !retryableNetworkError(reset) {
		t.Fatal("connection reset should be retryable")
	}
	if !retryableNetworkError(fmt.Errorf("dial tcp6: address api.kilo.ai: no suitable address found")) {
		t.Fatal("an IPv6-only dial failure must be retryable through another exit")
	}
	if retryableNetworkError(errors.New("invalid character 'x' looking for beginning of value")) {
		t.Fatal("a payload bug must not be retried forever")
	}
}

func TestParkedErrorTellsTheAgentWhenToRetry(t *testing.T) {
	err := newParkedError("every exit is cooling down", 42*time.Second)
	message := err.Error()
	if !strings.Contains(message, "retryable=true") || !strings.Contains(message, "retry_in=42s") {
		t.Fatalf("parked error must carry the retry hint, got %q", message)
	}
	if !isParkedError(err) || !retryableNetworkError(err) {
		t.Fatal("parked errors count as retryable")
	}
}

func TestFailureReportJoinsAttempts(t *testing.T) {
	report := &failureReport{}
	report.add("exit 1", errors.New("eof"))
	report.add("exit 2", errors.New("timeout"))
	if got := report.String(); got != "exit 1: eof | exit 2: timeout" {
		t.Fatalf("report = %q", got)
	}
	if (&failureReport{}).String() != "" {
		t.Fatal("an empty report renders as empty")
	}
}

func TestHealthParksThenRecovers(t *testing.T) {
	h := newHealth()
	if !h.usable("exit") {
		t.Fatal("a fresh exit is usable")
	}
	h.fail("exit", "429 from upstream")
	if h.usable("exit") {
		t.Fatal("a failed exit is parked")
	}
	if h.cooldownRemaining("exit") <= 0 {
		t.Fatal("cooldown should be positive")
	}
	if h.lastFailureFor("exit") != "429 from upstream" {
		t.Fatalf("last failure = %q", h.lastFailureFor("exit"))
	}
	// Cooldowns are bounded, never permanent.
	h.succeed("exit")
	if !h.usable("exit") {
		t.Fatal("a success must clear the cooldown")
	}
}

func TestHealthBackoffIsCapped(t *testing.T) {
	h := newHealth()
	for i := 0; i < 20; i++ {
		h.fail("exit", "boom")
	}
	if remaining := h.cooldownRemaining("exit"); remaining > 2*time.Minute+time.Second {
		t.Fatalf("backoff must stay capped at two minutes, got %s", remaining)
	}
}

func TestReadSocksFileSkipsJunk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "socks.txt")
	content := strings.Join([]string{
		"# ranked exits",
		"socks5://p0:p@127.0.0.1:20800",
		"",
		"http://10.0.0.1:3128",
		"not a proxy",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	exits, err := readSocksFile(path)
	if err != nil {
		t.Fatalf("readSocksFile: %v", err)
	}
	if len(exits) != 2 {
		t.Fatalf("exits = %v, want the two valid lines", exits)
	}
	entries := parseSocksEntries(exits)
	if len(entries) != 2 || entries[0].kind != "socks" {
		t.Fatalf("entries = %+v", entries)
	}
	if entries[0].httpTransport() == nil {
		t.Fatal("a socks exit must build a transport")
	}
}

func TestEntryLabelsAreDistinct(t *testing.T) {
	direct := &egressEntry{kind: "direct", label: "direct"}
	socks := &egressEntry{kind: "socks", label: "socks5://x:1"}
	if direct.label == socks.label {
		t.Fatal("labels must differ so health keys do not collide")
	}
	if direct.httpTransport().Proxy != nil {
		t.Fatal("a direct exit must not use a proxy")
	}
	_ = http.StatusOK
}

func TestFirstByteBudgetDefaults(t *testing.T) {
	rt := &runtime{}
	if rt.firstByte() != 60*time.Second {
		t.Fatalf("default first-byte budget = %s", rt.firstByte())
	}
	rt.cfg.FirstByteSeconds = 25
	if rt.firstByte() != 25*time.Second {
		t.Fatalf("first-byte budget = %s", rt.firstByte())
	}
}

func TestLocalUpstreamsNeverUseAPublicExit(t *testing.T) {
	for _, host := range []string{"localhost", "127.0.0.1", "::1", "10.0.0.5", "192.168.1.2", "172.16.4.4", "myserver.local"} {
		if !isLocalHost(host) {
			t.Fatalf("%s should be treated as a local target", host)
		}
	}
	for _, host := range []string{"api.kilo.ai", "8.8.8.8", "2001:4860:4860::8888"} {
		if isLocalHost(host) {
			t.Fatalf("%s must not be treated as local", host)
		}
	}
}

func TestEntryForLocalHostIsDirect(t *testing.T) {
	pool := &egressPool{mode: "auto", direct: &egressEntry{kind: "direct", label: "direct"}}
	pool.socks = []*egressEntry{{kind: "socks", label: "socks5://x:1"}}
	for _, policy := range []string{policyAuto, policyIPv6, policySocks} {
		if entry := pool.entryFor("127.0.0.1", policy); entry.kind != "direct" {
			t.Fatalf("policy %s routed a local target through %s", policy, entry.kind)
		}
	}
}
