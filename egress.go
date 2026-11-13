package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// egressPolicy decides which exits an endpoint may use.
const (
	policyAuto   = "auto"   // IPv6 /48 pool when the host has AAAA, else socks, else direct
	policyIPv6   = "ipv6"   // force the routed /48 pool
	policySocks  = "socks"  // force the SOCKS5 exit pool
	policyDirect = "direct" // the machine's own route
)

// egressEntry is one way out: a bound IPv6 source address, a SOCKS5 exit, or
// the machine's own route.
type egressEntry struct {
	kind  string
	label string
	ip    net.IP
	socks *url.URL

	once       sync.Once
	transport  *http.Transport
	notBefore  atomic.Int64 // unix nanos until this exit may be tried again
	failures   atomic.Int64
	lastReason atomic.Value // string

	// S-tier scoring: exponential moving average of time-to-first-byte, in
	// nanoseconds, plus a sample counter. Unmeasured exits score as "unknown"
	// and are explored, so a faster exit always takes over quickly.
	latencyEWMA atomic.Int64
	samples     atomic.Int64
	lastUsedMs  atomic.Int64
	inflight    atomic.Int64
}

func (e *egressEntry) httpTransport() *http.Transport {
	e.once.Do(func() {
		dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
		transport := &http.Transport{
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          16,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		}
		switch e.kind {
		case "ipv6":
			dialer.LocalAddr = &net.TCPAddr{IP: e.ip}
			transport.DialContext = dialer.DialContext
		case "socks":
			transport.Proxy = http.ProxyURL(e.socks)
			transport.DialContext = dialer.DialContext
		default:
			transport.DialContext = dialer.DialContext
		}
		e.transport = transport
	})
	return e.transport
}

func (e *egressEntry) usable() bool {
	return time.Now().UnixNano() >= e.notBefore.Load()
}

func (e *egressEntry) fail(reason string) {
	fails := e.failures.Add(1)
	wait := time.Duration(1<<uint(minInt(int(fails), 5))) * time.Second // 2s..32s
	if wait > 30*time.Second {
		wait = 30 * time.Second
	}
	e.notBefore.Store(time.Now().Add(wait).UnixNano())
	e.lastReason.Store(reason)
}

func (e *egressEntry) succeed() {
	e.failures.Store(0)
	e.notBefore.Store(0)
}

// recordLatency folds one time-to-first-byte measurement into the EWMA
// (alpha = 0.3: fast reaction to changes, still stable).
func (e *egressEntry) recordLatency(d time.Duration) {
	if d <= 0 {
		return
	}
	value := d.Nanoseconds()
	previous := e.latencyEWMA.Load()
	if previous == 0 {
		e.latencyEWMA.Store(value)
	} else {
		e.latencyEWMA.Store((previous*7 + value*3) / 10)
	}
	e.samples.Add(1)
	e.lastUsedMs.Store(time.Now().UnixMilli())
}

// score is the selection cost: lower is better. Unmeasured exits use a probe
// score so they get tried without outranking a known-fast exit.
func (e *egressEntry) score() int64 {
	if e.inflight.Load() > 0 && e.inflight.Load() >= 8 {
		return 1 << 62 // saturated: prefer another exit
	}
	latency := e.latencyEWMA.Load()
	if latency == 0 {
		return int64(120 * time.Millisecond) // explore, but behind proven-fast exits
	}
	penalty := e.failures.Load() * int64(150*time.Millisecond)
	return latency + penalty
}

// cooldownSeconds caps how long an exit can be parked. Rate limits on free
// tiers are usually hourly/daily, so parking must be bounded and re-probed:
// nothing stays out of the pool forever.
func (e *egressEntry) parkFor(reason string, rateLimited bool) {
	fails := e.failures.Add(1)
	wait := time.Duration(1<<uint(minInt(int(fails), 4))) * time.Second // 2s..16s
	if rateLimited {
		wait = 90 * time.Second
		if fails > 2 {
			wait = 300 * time.Second
		}
	}
	if wait > 5*time.Minute {
		wait = 5 * time.Minute
	}
	e.notBefore.Store(time.Now().Add(wait).UnixNano())
	e.lastReason.Store(reason)
}

func (e *egressEntry) reason() string {
	if value := e.lastReason.Load(); value != nil {
		if text, ok := value.(string); ok {
			return text
		}
	}
	return ""
}

// egressPool owns the exit pools and rotates inside each one.
type egressPool struct {
	mode      string
	prefixes  []*net.IPNet
	iface     string
	poolSize  int
	socksFile string

	mu       sync.Mutex
	ipv6     []*egressEntry
	socks    []*egressEntry
	direct   *egressEntry
	loadedAt time.Time

	counter6     atomic.Uint64
	counterSocks atomic.Uint64

	aaaaMu  sync.Mutex
	aaaa    map[string]bool
	aaaaHit map[string]time.Time

	socksMu     sync.Mutex
	socksLoaded time.Time
}

func newEgressPool(cfg egressConfig, endpoints []endpointConfig) (*egressPool, error) {
	pool := &egressPool{
		mode:      cfg.Mode,
		iface:     cfg.Interface,
		poolSize:  cfg.PoolSize,
		socksFile: cfg.SocksFile,
		direct:    &egressEntry{kind: "direct", label: "direct"},
		aaaa:      make(map[string]bool),
		aaaaHit:   make(map[string]time.Time),
	}
	for _, raw := range cfg.Prefixes {
		_, network, errParse := net.ParseCIDR(strings.TrimSpace(raw))
		if errParse != nil {
			return nil, fmt.Errorf("egress.prefixes: %q is not a CIDR: %w", raw, errParse)
		}
		pool.prefixes = append(pool.prefixes, network)
	}
	startupExits := append([]string{}, cfg.Socks...)
	if cfg.SocksFile != "" {
		fileExits, errRead := readSocksFile(cfg.SocksFile)
		if errRead != nil {
			hostLog("warn", fmt.Sprintf("cliproxy-rotator: socks_file %s: %v", cfg.SocksFile, errRead))
		}
		startupExits = append(startupExits, fileExits...)
	}
	if len(startupExits) == 0 && cfg.SocksFallback != "" {
		fallbackExits, errFallback := readSocksFile(cfg.SocksFallback)
		if errFallback != nil {
			hostLog("warn", fmt.Sprintf("cliproxy-rotator: socks_fallback %s: %v", cfg.SocksFallback, errFallback))
		} else {
			hostLog("info", fmt.Sprintf("cliproxy-rotator: no live socks list, seeded %d exits from the bundled pool %s", len(fallbackExits), cfg.SocksFallback))
			startupExits = append(startupExits, fallbackExits...)
		}
	}
	pool.reloadSocksLocked(startupExits, true)
	pool.socksLoaded = time.Now()
	pool.refreshIPv6(true)

	// Pre-resolve the endpoint hosts now so request time is never spent on DNS.
	for i := range endpoints {
		if parsed, errParse := url.Parse(endpoints[i].URL); errParse == nil {
			host := strings.ToLower(parsed.Hostname())
			if host != "" && pool.mode != policyDirect {
				go pool.probeHostAAAA(host)
			}
		}
	}
	hostLog("info", fmt.Sprintf("cliproxy-rotator: egress mode %s: %d IPv6 sources, %d SOCKS exits", pool.mode, len(pool.ipv6), len(pool.socks)))
	return pool, nil
}

// acquire marks the entry busy so scoring spreads 30-way concurrency.
func (e *egressEntry) acquire() { e.inflight.Add(1) }
func (e *egressEntry) release() { e.inflight.Add(-1) }

func (p *egressPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.ipv6 {
		if p.ipv6[i].transport != nil {
			p.ipv6[i].transport.CloseIdleConnections()
		}
	}
	for i := range p.socks {
		if p.socks[i].transport != nil {
			p.socks[i].transport.CloseIdleConnections()
		}
	}
	if p.direct != nil && p.direct.transport != nil {
		p.direct.transport.CloseIdleConnections()
	}
}

// isLocalHost reports whether host is loopback, link-local or RFC1918 space.
func isLocalHost(host string) bool {
	cleaned := strings.ToLower(strings.TrimSuffix(strings.TrimSuffix(host, "."), ""))
	if cleaned == "localhost" || strings.HasSuffix(cleaned, ".localhost") || strings.HasSuffix(cleaned, ".local") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()
	}
	return false
}

// entryFor picks an exit for host under the endpoint's policy, skipping exits
// parked by recent failures.
func (p *egressPool) entryFor(host, policy string) *egressEntry {
	if host == "" {
		return p.direct
	}
	if isLocalHost(host) {
		// Loopback/LAN upstreams (a local server, a self-hosted model) must
		// never be pushed through a public exit: the exit cannot reach them.
		return p.direct
	}
	preferIPv6 := p.mode == "ipv6" || p.mode == "auto"
	switch policy {
	case policyDirect:
		return p.direct
	case policySocks:
		if entry := p.nextSocks(); entry != nil {
			return entry
		}
		return p.direct
	case policyIPv6:
		if entry := p.nextIPv6(); entry != nil {
			return entry
		}
		if entry := p.nextSocks(); entry != nil {
			return entry
		}
		return p.direct
	}
	// auto
	if preferIPv6 && p.hostHasIPv6(host) {
		if entry := p.nextIPv6(); entry != nil {
			return entry
		}
	}
	if entry := p.nextSocks(); entry != nil {
		return entry
	}
	if preferIPv6 {
		if entry := p.nextIPv6(); entry != nil {
			return entry
		}
	}
	return p.direct
}

// selectionWindow is how many candidate exits are scored per request. A small
// window keeps selection O(window) (never blocks) while still letting the
// fastest exit win consistently.
const selectionWindow = 8

// pick returns the best usable exit for host under policy, plus the shortest
// cooldown when everything is parked (so callers can say "retry in Xs").
func (p *egressPool) pick(host, policy string) (*egressEntry, time.Duration) {
	candidates := p.candidates(host, policy, selectionWindow)
	var best *egressEntry
	var bestScore int64
	var soonest time.Duration
	for _, entry := range candidates {
		if !entry.usable() {
			if remaining := time.Until(time.Unix(0, entry.notBefore.Load())); remaining > 0 {
				if soonest == 0 || remaining < soonest {
					soonest = remaining
				}
			}
			continue
		}
		score := entry.score()
		if best == nil || score < bestScore {
			best = entry
			bestScore = score
		}
	}
	return best, soonest
}

// candidates lists the exits allowed for host under policy, starting at the
// rotation cursor so equal-score exits are still load-balanced.
func (p *egressPool) candidates(host, policy string, limit int) []*egressEntry {
	var ipv6, socks []*egressEntry
	preferIPv6 := p.mode == "ipv6" || p.mode == "auto"
	useIPv6 := policy == policyIPv6 || (policy == policyAuto && preferIPv6 && p.hostHasIPv6(host))
	useSocks := policy == policySocks || (policy == policyAuto && !(useIPv6 && policy == policyAuto)) || policy == policyIPv6
	switch policy {
	case policyDirect:
		return []*egressEntry{p.direct}
	case policySocks:
		useIPv6, useSocks = false, true
	case policyIPv6:
		useIPv6, useSocks = true, true
	}
	if useIPv6 {
		p.refreshIPv6(false)
		p.mu.Lock()
		if count := len(p.ipv6); count > 0 {
			start := int(p.counter6.Add(1)-1) % count
			for i := 0; i < count && len(ipv6) < limit; i++ {
				ipv6 = append(ipv6, p.ipv6[(start+i)%count])
			}
		}
		p.mu.Unlock()
	}
	if useSocks {
		p.reloadSocksFileIfChanged()
		p.socksMu.Lock()
		if count := len(p.socks); count > 0 {
			start := int(p.counterSocks.Add(1)-1) % count
			for i := 0; i < count && len(socks) < limit; i++ {
				socks = append(socks, p.socks[(start+i)%count])
			}
		}
		p.socksMu.Unlock()
	}
	out := append(ipv6, socks...)
	if len(out) == 0 {
		return []*egressEntry{p.direct}
	}
	return out
}

func (p *egressPool) nextIPv6() *egressEntry {
	p.refreshIPv6(false)
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.ipv6) == 0 {
		return nil
	}
	idx := int(p.counter6.Add(1)-1) % len(p.ipv6)
	return p.ipv6[idx]
}

func (p *egressPool) nextSocks() *egressEntry {
	p.reloadSocksFileIfChanged()
	p.socksMu.Lock()
	count := len(p.socks)
	var entry *egressEntry
	if count > 0 {
		idx := int(p.counterSocks.Add(1)-1) % count
		entry = p.socks[idx]
	}
	p.socksMu.Unlock()
	return entry
}

// hostHasIPv6 reports whether host publishes an AAAA record (cached 10 min).
func (p *egressPool) hostHasIPv6(host string) bool {
	p.aaaaMu.Lock()
	if value, ok := p.aaaa[host]; ok && time.Since(p.aaaaHit[host]) < 10*time.Minute {
		p.aaaaMu.Unlock()
		return value
	}
	if value, ok := p.aaaa[host]; ok {
		p.aaaaMu.Unlock()
		go p.probeHostAAAA(host)
		return value
	}
	p.aaaaMu.Unlock()
	return p.probeHostAAAA(host)
}

// probeHostAAAA resolves AAAA and caches the result; never blocks long.
func (p *egressPool) probeHostAAAA(host string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	ips, errLookup := net.DefaultResolver.LookupIP(ctx, "ip6", host)
	has := errLookup == nil && len(ips) > 0
	p.aaaaMu.Lock()
	p.aaaa[host] = has
	p.aaaaHit[host] = time.Now()
	p.aaaaMu.Unlock()
	return has
}

// reloadSocksFileIfChanged re-reads the socks list when the file changes.
func (p *egressPool) reloadSocksFileIfChanged() {
	if p.socksFile == "" {
		return
	}
	p.socksMu.Lock()
	if time.Since(p.socksLoaded) < 60*time.Second {
		p.socksMu.Unlock()
		return
	}
	exits, errRead := readSocksFile(p.socksFile)
	if errRead != nil {
		p.socksMu.Unlock()
		return
	}
	entries := parseSocksEntries(exits)
	parsed := exits
	p.socks = entries
	p.socksLoaded = time.Now()
	p.socksMu.Unlock()
	if len(parsed) > 0 {
		hostLog("info", fmt.Sprintf("cliproxy-rotator: reloaded socks list from %s: %d exits", p.socksFile, len(entries)))
	}
}

func (p *egressPool) reloadSocksLocked(exits []string, startup bool) {
	entries := parseSocksEntriesOrWarn(exits)
	p.socksMu.Lock()
	p.socks = entries
	p.socksMu.Unlock()
	_ = startup
}

// refreshIPv6 re-enumerates the routed prefixes' local addresses, at most once
// every 30 seconds unless forced.
func (p *egressPool) refreshIPv6(force bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !force && !p.loadedAt.IsZero() && time.Since(p.loadedAt) < 30*time.Second {
		return
	}
	addrs, errAddrs := localIPv6Addresses(p.prefixes, p.iface)
	if errAddrs != nil {
		hostLog("warn", "cliproxy-rotator: enumerating IPv6 addresses: "+errAddrs.Error())
	}
	limit := p.poolSize
	if limit > 0 && len(addrs) > limit {
		addrs = addrs[:limit]
	}
	byLabel := make(map[string]*egressEntry, len(p.ipv6))
	for _, entry := range p.ipv6 {
		byLabel[entry.label] = entry
	}
	entries := make([]*egressEntry, 0, len(addrs))
	for _, ip := range addrs {
		if existing := byLabel[ip.String()]; existing != nil {
			// Reuse the same entry so its warm transport, latency history and
			// cooldown survive a pool refresh.
			entries = append(entries, existing)
			continue
		}
		entries = append(entries, &egressEntry{kind: "ipv6", label: ip.String(), ip: ip})
	}
	p.ipv6 = entries
	p.loadedAt = time.Now()
}

// localIPv6Addresses lists global IPv6 addresses that fall inside the routed
// prefixes (or, with no prefixes configured, every global address).
func localIPv6Addresses(prefixes []*net.IPNet, ifaceName string) ([]net.IP, error) {
	var interfaces []net.Interface
	if ifaceName != "" {
		iface, errIface := net.InterfaceByName(ifaceName)
		if errIface != nil {
			return nil, errIface
		}
		interfaces = []net.Interface{*iface}
	} else {
		all, errAll := net.Interfaces()
		if errAll != nil {
			return nil, errAll
		}
		interfaces = all
	}
	seen := make(map[string]net.IP)
	for _, iface := range interfaces {
		addrs, errAddrs := iface.Addrs()
		if errAddrs != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipNet.IP.To16()
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			if len(prefixes) > 0 {
				matched := false
				for _, prefix := range prefixes {
					if prefix.Contains(ip) {
						matched = true
						break
					}
				}
				if !matched {
					continue
				}
			}
			seen[ip.String()] = ip
		}
	}
	out := make([]net.IP, 0, len(seen))
	for _, ip := range seen {
		out = append(out, ip)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, nil
}

func readSocksFile(path string) ([]string, error) {
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return nil, errOpen
	}
	defer file.Close()
	var out []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// A file written by an external pool can contain anything; drop entries
		// this plugin cannot dial instead of counting them as usable exits.
		if _, errParse := parseSocksEntry(line); errParse != nil {
			continue
		}
		out = append(out, line)
	}
	return out, scanner.Err()
}

func parseSocksEntries(raw []string) []*egressEntry {
	var out []*egressEntry
	for _, line := range raw {
		entry, errParse := parseSocksEntry(line)
		if errParse != nil {
			hostLog("warn", "cliproxy-rotator: skipping socks exit "+errParse.Error())
			continue
		}
		out = append(out, entry)
	}
	return out
}

func parseSocksEntriesOrWarn(raw []string) []*egressEntry {
	return parseSocksEntries(raw)
}

func parseSocksEntry(raw string) (*egressEntry, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, fmt.Errorf("empty exit")
	}
	if !strings.Contains(value, "://") {
		value = "socks5://" + value
	}
	if strings.HasPrefix(value, "socks5h://") {
		value = strings.Replace(value, "socks5h://", "socks5://", 1)
	}
	parsed, errParse := url.Parse(value)
	if errParse != nil {
		return nil, errParse
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("missing host")
	}
	switch parsed.Scheme {
	case "socks5", "socks5h", "http", "https":
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q (socks5 or http)", parsed.Scheme)
	}
	if parsed.Port() == "" {
		parsed.Host = net.JoinHostPort(parsed.Hostname(), "1080")
	}
	return &egressEntry{kind: "socks", label: parsed.Redacted(), socks: parsed}, nil
}

// rotatingClient hands every request to the exit chosen for its host.
type rotatingClient struct {
	pool   *egressPool
	client *http.Client
	policy map[string]string // host -> egress policy
}

type rotatingTransport struct {
	pool   *egressPool
	policy map[string]string
}

func (t *rotatingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()
	policy := policyAuto
	if p, ok := t.policy[strings.ToLower(host)]; ok {
		policy = p
	}
	entry, cooldown := t.pool.pick(host, policy)
	if entry == nil {
		if cooldown > 0 {
			return nil, newParkedError(fmt.Sprintf("all %s exits are cooling down", host), cooldown)
		}
		return nil, fmt.Errorf("no egress available for %s", host)
	}
	if socketDebug() != "" || req.Header.Get("X-Rotator-Debug") != "" {
		hostLog("debug", "cliproxy-rotator: egress "+entry.label+" ("+policy+") -> "+req.URL.String())
	}
	entry.acquire()
	started := time.Now()
	resp, errDo := entry.httpTransport().RoundTrip(req)
	entry.release()
	if errDo != nil {
		entry.parkFor(errDo.Error(), false)
		return nil, errDo
	}
	entry.recordLatency(time.Since(started))
	entry.succeed()
	return resp, nil
}

func newRotatingClient(pool *egressPool, cfg pluginConfig, hosts map[string]string) *rotatingClient {
	return &rotatingClient{
		pool:   pool,
		policy: hosts,
		client: &http.Client{
			Transport: &rotatingTransport{pool: pool, policy: hosts},
		},
	}
}