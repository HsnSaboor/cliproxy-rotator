package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestLatencyBreakdown measures where request time goes: DNS, dial, TLS, wait
// for first byte and total. It is opt-in (ROTATOR_LATENCY=1) because it needs
// the live exit pools.
func TestLatencyBreakdown(t *testing.T) {
	if os.Getenv("ROTATOR_LATENCY") == "" {
		t.Skip("set ROTATOR_LATENCY=1 to run")
	}
	home := os.Getenv("HOME")
	cfg := egressConfig{
		Mode:      "auto",
		Prefixes:  []string{"2001:470:52fa::/48"},
		Interface: "he-ipv6",
		PoolSize:  512,
		SocksFile: home + "/.cache/zen-egress/socks.txt",
	}
	endpoints := []endpointConfig{{Name: "kilo", URL: "https://api.kilo.ai/api/gateway/v1"}}
	pool, errPool := newEgressPool(cfg, endpoints)
	if errPool != nil {
		t.Fatalf("pool: %v", errPool)
	}
	defer pool.Close()
	time.Sleep(3 * time.Second) // let the background probe settle

	body := []byte(`{"model":"particle-ai/particle-1:free","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	run := func(policy string, n int) {
		fmt.Printf("\n--- %s ---\n", policy)
		var rows []string
		var dial, tlsDur, ttfb, total []time.Duration
		reused := 0
		for i := 0; i < n; i++ {
			entry, _ := pool.pick("api.kilo.ai", policy)
			if entry == nil {
				fmt.Println("no exit")
				return
			}
			req, _ := http.NewRequest(http.MethodPost, "https://api.kilo.ai/api/gateway/v1/chat/completions", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			var (
				tDNS, tConnStart, tTLSStart, tFirst time.Time
				dnsD, connD, tlsD                   time.Duration
				wasReused                           bool
			)
			trace := &httptrace.ClientTrace{
				DNSStart:             func(httptrace.DNSStartInfo) { tDNS = time.Now() },
				DNSDone:              func(httptrace.DNSDoneInfo) { dnsD = time.Since(tDNS) },
				ConnectStart:         func(_, _ string) { tConnStart = time.Now() },
				ConnectDone:          func(_, _ string, _ error) { connD = time.Since(tConnStart) },
				TLSHandshakeStart:    func() { tTLSStart = time.Now() },
				TLSHandshakeDone:     func(tls.ConnectionState, error) { tlsD = time.Since(tTLSStart) },
				GotConn:              func(info httptrace.GotConnInfo) { wasReused = info.Reused },
				GotFirstResponseByte: func() { ttfb = append(ttfb, time.Since(tFirst)) },
			}
			req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
			client := &http.Client{Transport: &singleEntryTransport{entry: entry}}
			start := time.Now()
			tFirst = start
			resp, errDo := client.Do(req)
			if errDo != nil {
				rows = append(rows, fmt.Sprintf("%-6s ERR %v", entry.kind, errDo))
				continue
			}
			resp.Body.Close()
			end := time.Now()
			dial = append(dial, connD)
			tlsDur = append(tlsDur, tlsD)
			_ = dnsD
			if wasReused {
				reused++
			}
			d := end.Sub(start)
			total = append(total, d)
			rows = append(rows, fmt.Sprintf("%-6s %-26s reuse=%-5v dns=%4.0fms conn=%5.0fms tls=%5.0fms total=%6.0fms",
				entry.kind, truncLabel(entry.label), wasReused,
				float64(dnsD.Microseconds())/1000, float64(connD.Microseconds())/1000,
				float64(tlsD.Microseconds())/1000, float64(d.Microseconds())/1000))
			time.Sleep(150 * time.Millisecond)
		}
		for _, r := range rows {
			fmt.Println("  " + r)
		}
		fmt.Printf("  reused=%d/%d  conn p50=%s  tls p50=%s  total p50=%s p95=%s\n",
			reused, n, pct(dial, 50), pct(tlsDur, 50), pct(total, 50), pct(total, 95))
	}
	run(policySocks, 6)
	run(policyIPv6, 6)
	run(policyDirect, 3)
}

func pct(v []time.Duration, p int) time.Duration {
	if len(v) == 0 {
		return 0
	}
	s := append([]time.Duration{}, v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := (len(s) - 1) * p / 100
	return s[idx]
}

func truncLabel(l string) string {
	if len(l) > 28 {
		return strings.ReplaceAll(l[:28], " ", "")
	}
	return strings.ReplaceAll(l, " ", "")
}
