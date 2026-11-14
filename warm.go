package main

import (
	"context"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Latency engineering
// -------------------
// A request through a rotating exit used to pay for everything twice: the TCP
// and TLS handshake through the proxy hop (about 320ms measured over SOCKS),
// plus the upstream's own time to first byte. Three mechanisms remove that
// cost from the request path:
//
//  1. A warm ring: a handful of the best exits stay selected for a while, so
//     their connections live in the transport's idle pool and each request
//     finds a ready socket (TLS reuse, no dial).
//  2. A prewarm loop that re-uses those sockets every warm_interval_seconds,
//     so they never fall out of the idle pool while traffic pauses.
//  3. Hedging: if no response headers arrive within hedge_delay, a second exit
//     is raced and the first one to answer wins.

const (
	defaultWarmSize  = 4
	defaultWarmEvery = 20 * time.Second
	defaultRingTTL   = 120 * time.Second
)

type ringKey struct {
	host   string
	policy string
}

// warmRing is a small, stable subset of the pool whose connections stay hot.
type warmRing struct {
	entries []*egressEntry
	built   time.Time
	cursor  atomic.Uint64
}

// next returns the next usable, unsaturated entry of the ring, or nil.
func (ring *warmRing) next() *egressEntry {
	count := len(ring.entries)
	if count == 0 {
		return nil
	}
	start := int(ring.cursor.Add(1)-1) % count
	for i := 0; i < count; i++ {
		entry := ring.entries[(start+i)%count]
		if entry == nil || !entry.usable() || entry.saturated() {
			continue
		}
		return entry
	}
	return nil
}

func (ring *warmRing) stale(ttl time.Duration) bool {
	if time.Since(ring.built) > ttl {
		return true
	}
	for _, entry := range ring.entries {
		if entry == nil || !entry.usable() {
			return true
		}
	}
	return false
}

// ring returns the warm ring for host/policy, rebuilding it when it is stale.
// Called on the request path, so the work is bounded by the ring size.
func (p *egressPool) ring(host, policy string) *warmRing {
	if p.warmSize <= 0 {
		return nil
	}
	key := ringKey{host: host, policy: policy}
	p.ringsMu.Lock()
	ring, ok := p.rings[key]
	p.ringsMu.Unlock()
	if ok && !ring.stale(p.ringTTL) {
		return ring
	}
	rebuilt := p.buildRing(host, policy)
	if rebuilt == nil {
		return nil
	}
	p.ringsMu.Lock()
	p.rings[key] = rebuilt
	p.ringsMu.Unlock()
	return rebuilt
}

// buildRing selects the warm set: the fastest exits that are currently usable,
// keeping whatever is still healthy so their warm sockets survive the rebuild.
func (p *egressPool) buildRing(host, policy string) *warmRing {
	size := p.warmSize
	if size <= 0 {
		size = defaultWarmSize
	}
	// Ask for enough candidates to fill the ring: the scoring window alone
	// (selectionWindow) is smaller than a large warm ring.
	window := selectionWindow
	if size*2 > window {
		window = size * 2
	}
	candidates := p.candidates(host, policy, window)
	if len(candidates) == 0 {
		return nil
	}
	type scored struct {
		entry *egressEntry
		score int64
	}
	usable := make([]scored, 0, len(candidates))
	for _, entry := range candidates {
		if entry == nil || !entry.usable() {
			continue
		}
		usable = append(usable, scored{entry: entry, score: entry.score()})
	}
	if len(usable) == 0 {
		return nil
	}
	sort.SliceStable(usable, func(i, j int) bool { return usable[i].score < usable[j].score })
	entries := make([]*egressEntry, 0, size)
	for i := 0; i < len(usable) && len(entries) < size; i++ {
		entries = append(entries, usable[i].entry)
	}
	return &warmRing{entries: entries, built: time.Now()}
}

// warmLoop keeps the ring sockets alive. It only ever touches the ring's own
// exits, so it cannot disturb rotation in the wider pool.
func (p *egressPool) warmLoop() {
	interval := p.warmEvery
	if interval <= 0 {
		interval = defaultWarmEvery
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.warmStop:
			return
		case <-ticker.C:
			p.warmOnce()
		}
	}
}

// warmOnce refreshes every ring's sockets with one cheap GET each, so the
// TLS connection is reused instead of renegotiated on the next request.
func (p *egressPool) warmOnce() {
	target := p.warmURL
	if target == "" {
		return
	}
	p.ringsMu.Lock()
	rings := make([]*warmRing, 0, len(p.rings))
	for _, ring := range p.rings {
		rings = append(rings, ring)
	}
	p.ringsMu.Unlock()
	var wg sync.WaitGroup
	for _, ring := range rings {
		for _, entry := range ring.entries {
			if entry == nil || !entry.usable() {
				continue
			}
			wg.Add(1)
			go func(entry *egressEntry) {
				defer wg.Done()
				p.preWarm(entry, target)
			}(entry)
		}
	}
	wg.Wait()
}

// preWarm performs a small GET through the exit so its connection stays open
// in the transport's idle pool. Failures park the exit, which invalidates the
// ring on the next request.
func (p *egressPool) preWarm(entry *egressEntry, target string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, errNew := http.NewRequestWithContext(ctx, http.MethodHead, target, nil)
	if errNew != nil {
		return
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "cliproxy-rotator/warm")
	client := &http.Client{Transport: &singleEntryTransport{entry: entry}, Timeout: 10 * time.Second}
	resp, errDo := client.Do(request)
	if errDo != nil {
		if unreachableExit(errDo) {
			entry.parkDead(errDo.Error())
		}
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		// HEAD is the cheap keep-alive; if a gateway refuses it, an OPTIONS
		// probe still refreshes the connection without a payload.
		entry.succeed()
		return
	}
	if resp.StatusCode < 500 {
		entry.succeed()
	}
}

// closeRings stops the warmer.
func (p *egressPool) closeRings() {
	if p.warmStop == nil {
		return
	}
	select {
	case <-p.warmStop:
	default:
		close(p.warmStop)
	}
}

// cancelBody cancels the losing attempt's context when the winning body is
// closed, so a hedge never leaks a connection.
type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b cancelBody) Close() error {
	err := b.ReadCloser.Close()
	if b.cancel != nil {
		b.cancel()
	}
	return err
}

// attemptHandle owns the cancel func of one hedged attempt, so the winner can
// be told apart from the losers without comparing funcs.
type attemptHandle struct {
	cancel context.CancelFunc
}

// configureWarmRing applies the configuration knobs. Zero values keep the
// defaults, so an untouched config still gets warm connections.
func (p *egressPool) configureWarmRing(cfg egressConfig) {
	size, every, ttl := effectiveWarm(cfg, p.selection)
	p.warmSize = size
	p.warmEvery = every
	p.ringTTL = ttl
}

// effectiveWarm resolves the warm-ring settings, applying the defaults for
// the values the configuration leaves at zero. The pool and the reuse check
// both use it, so they always agree.
func effectiveWarm(cfg egressConfig, selection string) (int, time.Duration, time.Duration) {
	size := cfg.WarmSize
	if size == 0 {
		size = defaultWarmSize
		if selection == selectionRoundRobin {
			// Rotation still spreads rate limits: a wider ring keeps more
			// source addresses in play without paying a handshake each time.
			size = defaultWarmSize + 2
		}
	}
	if size < 0 {
		size = 0 // negative disables the ring entirely
	}
	every := time.Duration(cfg.WarmIntervalSeconds) * time.Second
	if every <= 0 {
		every = defaultWarmEvery
	}
	ttl := time.Duration(cfg.RingTTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = defaultRingTTL
	}
	return size, every, ttl
}

// warmTarget is the cheap endpoint used to keep ring sockets alive. The model
// list is small and does not consume tokens.
func warmTarget(endpoints []endpointConfig) string {
	for i := range endpoints {
		base := strings.TrimRight(strings.TrimSpace(endpoints[i].URL), "/")
		if base == "" {
			continue
		}
		if strings.HasSuffix(base, "/v1") || strings.Contains(base, "/v1/") {
			return base + "/models"
		}
		return base + "/models"
	}
	return ""
}

// rateLimitCooldown reports how long an exit is parked, so a rate-limited
// request can tell the agent when to retry instead of just failing.
func rateLimitCooldown(entry *egressEntry) time.Duration {
	if entry == nil {
		return 0
	}
	if remaining := time.Until(time.Unix(0, entry.notBefore.Load())); remaining > 0 {
		return remaining
	}
	return 0
}
