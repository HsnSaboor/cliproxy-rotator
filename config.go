package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

// endpointConfig is one OpenAI-compatible upstream.
type endpointConfig struct {
	Name    string            `yaml:"name"`
	URL     string            `yaml:"url"`
	APIKeys []string          `yaml:"api_keys"`
	Headers map[string]string `yaml:"headers"`
	Models  []string          `yaml:"models"`
	// Egress overrides the pool for this endpoint: auto (default), ipv6, socks, direct.
	Egress string `yaml:"egress"`
}

// egressConfig selects where requests leave from.
type egressConfig struct {
	// Mode is "ipv6" (rotating source address out of the routed prefix pool),
	// "socks" (rotating SOCKS5 exits, e.g. the sing-box/zen pool) or "direct".
	Mode      string   `yaml:"mode"`
	Prefixes  []string `yaml:"prefixes"`
	Interface string   `yaml:"interface"`
	PoolSize  int      `yaml:"pool_size"`
	Socks     []string `yaml:"socks"`
	SocksFile string   `yaml:"socks_file"`
	// SocksFallback is read when SocksFile is missing or yields no usable exit,
	// so a fresh install starts from the warm pool shipped in this repository.
	SocksFallback string `yaml:"socks_fallback"`
	// Selection decides how an exit is picked from the pool:
	// "round-robin" (default) walks the pool in order, so consecutive
	// requests leave from different source addresses; "fastest" prefers the
	// exit with the lowest observed latency.
	Selection string `yaml:"selection"`
	// WarmSize is how many exits keep a live TLS connection ("warm ring"):
	// 0 selects the default (4, or 6 with round-robin), negative disables.
	WarmSize int `yaml:"warm_pool_size"`
	// WarmIntervalSeconds is the keep-alive cadence for those sockets.
	WarmIntervalSeconds int `yaml:"warm_interval_seconds"`
	// RingTTLSeconds is how long a warm set is kept before re-ranking.
	RingTTLSeconds int `yaml:"ring_ttl_seconds"`
}

// modelConfig publishes one model id on the rotated provider.
type modelConfig struct {
	ID                  string   `yaml:"id"`
	Upstream            string   `yaml:"upstream"`
	Endpoint            string   `yaml:"endpoint"`
	DisplayName         string   `yaml:"display_name"`
	Description         string   `yaml:"description"`
	OwnedBy             string   `yaml:"owned_by"`
	ContextLength       int64    `yaml:"context_length"`
	MaxCompletionTokens int64    `yaml:"max_completion_tokens"`
	InputModalities     []string `yaml:"input_modalities"`
	OutputModalities    []string `yaml:"output_modalities"`
	SupportedParameters []string `yaml:"supported_parameters"`
	ThinkingLevels      []string `yaml:"thinking_levels"`
}

type pluginConfig struct {
	Enabled   *bool            `yaml:"enabled"`
	Provider  string           `yaml:"provider"`
	UserAgent string           `yaml:"user_agent"`
	Endpoints []endpointConfig `yaml:"endpoints"`
	Egress    egressConfig     `yaml:"egress"`
	Models    []modelConfig    `yaml:"models"`

	// Request time budgets: nothing waits forever. A request is abandoned with
	// a clear retryable error as soon as its budget is exhausted.
	TotalTimeoutSeconds int `yaml:"total_timeout_seconds"`      // whole request (non-stream), default 120
	FirstByteSeconds    int `yaml:"first_byte_timeout_seconds"` // stream: time to first upstream byte, default 60
	IdleSeconds         int `yaml:"idle_timeout_seconds"`       // stream: idle gap allowed between chunks, default 120
	MaxAttempts         int `yaml:"max_attempts"`               // total tries across slots, default 6
	// HedgeDelayMs races a second exit when the first has not produced
	// response headers within this delay. 0 keeps single-exit attempts, a
	// negative value disables hedging explicitly.
	HedgeDelayMs int `yaml:"hedge_delay_ms"`
}

// slot is one (endpoint, api key) rotation entry.
type slot struct {
	endpoint *endpointConfig
	apiKey   string
}

// runtime is the immutable view built from the plugin config.
type runtime struct {
	cfg       pluginConfig
	provider  string
	slots     []slot
	models    []modelConfig
	egress    *egressPool
	client    *rotatingClient
	health    *health
	epCounter atomic.Uint64
}

func defaultConfig() pluginConfig {
	enabled := true
	return pluginConfig{
		Enabled:             &enabled,
		Provider:            "rotator",
		Endpoints:           nil,
		Egress:              egressConfig{Mode: "auto"},
		TotalTimeoutSeconds: 120,
		FirstByteSeconds:    60,
		IdleSeconds:         120,
		MaxAttempts:         6,
		HedgeDelayMs:        550,
	}
}

func decodeConfig(raw []byte) (pluginConfig, error) {
	cfg := defaultConfig()
	if len(raw) > 0 {
		if errUnmarshal := yaml.Unmarshal(raw, &cfg); errUnmarshal != nil {
			return pluginConfig{}, fmt.Errorf("decode plugin config: %w", errUnmarshal)
		}
	}
	if cfg.Provider == "" {
		cfg.Provider = "rotator"
	}
	if cfg.TotalTimeoutSeconds <= 0 {
		cfg.TotalTimeoutSeconds = 120
	}
	if cfg.FirstByteSeconds <= 0 {
		cfg.FirstByteSeconds = 60
	}
	if cfg.IdleSeconds <= 0 {
		cfg.IdleSeconds = 120
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 6
	}
	if cfg.HedgeDelayMs == 0 {
		cfg.HedgeDelayMs = 550
	}
	cfg.Egress.Mode = strings.ToLower(strings.TrimSpace(cfg.Egress.Mode))
	switch cfg.Egress.Mode {
	case "", "auto", "ipv6+fallback":
		cfg.Egress.Mode = "auto"
	case "ipv6", "v6":
		cfg.Egress.Mode = "ipv6"
	case "socks", "socks5", "zen":
		cfg.Egress.Mode = "socks"
	case "direct", "none":
		cfg.Egress.Mode = "direct"
	default:
		return pluginConfig{}, fmt.Errorf("egress.mode %q is not one of auto, ipv6, socks, direct", cfg.Egress.Mode)
	}
	cfg.Egress.Selection = strings.ToLower(strings.TrimSpace(cfg.Egress.Selection))
	switch cfg.Egress.Selection {
	case "", selectionRoundRobin, "roundrobin", "rr":
		cfg.Egress.Selection = selectionRoundRobin
	case selectionFastest, "latency", "best":
		cfg.Egress.Selection = selectionFastest
	default:
		return pluginConfig{}, fmt.Errorf("egress.selection %q is not one of round-robin, fastest", cfg.Egress.Selection)
	}
	if len(cfg.Endpoints) == 0 {
		return pluginConfig{}, fmt.Errorf("endpoints is empty: at least one upstream URL is required")
	}
	for i := range cfg.Endpoints {
		ep := &cfg.Endpoints[i]
		ep.URL = strings.TrimRight(strings.TrimSpace(ep.URL), "/")
		if ep.URL == "" {
			return pluginConfig{}, fmt.Errorf("endpoints[%d].url is required", i)
		}
		if _, errURL := url.Parse(ep.URL); errURL != nil {
			return pluginConfig{}, fmt.Errorf("endpoints[%d].url: %w", i, errURL)
		}
		ep.Egress = strings.ToLower(strings.TrimSpace(ep.Egress))
		switch ep.Egress {
		case "", policyAuto:
			ep.Egress = policyAuto
		case "v6":
			ep.Egress = policyIPv6
		case "socks5", "zen":
			ep.Egress = policySocks
		case policyIPv6, policySocks, policyDirect:
		default:
			return pluginConfig{}, fmt.Errorf("endpoints[%d].egress %q is not one of auto, ipv6, socks, direct", i, ep.Egress)
		}
		if ep.Name == "" {
			if parsed, errParse := url.Parse(ep.URL); errParse == nil {
				ep.Name = parsed.Host
			}
		}
	}
	return cfg, nil
}

func configure(raw []byte) error {
	var req lifecycleRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return fmt.Errorf("decode lifecycle request: %w", errUnmarshal)
		}
	}
	cfg, errDecode := decodeConfig(req.ConfigYAML)
	if errDecode != nil {
		return errDecode
	}
	previous, _ := activeConfig.Load().(*runtime)
	rt, errBuild := buildRuntime(cfg, previous)
	if errBuild != nil {
		return errBuild
	}
	activeConfig.Store(rt)
	if previous != nil && previous.egress != nil && previous.egress != rt.egress {
		previous.egress.Close()
	}
	return nil
}

func buildRuntime(cfg pluginConfig, previous *runtime) (*runtime, error) {
	rt := &runtime{cfg: cfg, provider: cfg.Provider, models: cfg.Models}
	for i := range cfg.Endpoints {
		ep := &cfg.Endpoints[i]
		keys := ep.APIKeys
		if len(keys) == 0 {
			keys = []string{""} // anonymous
		}
		for _, key := range keys {
			rt.slots = append(rt.slots, slot{endpoint: ep, apiKey: strings.TrimSpace(key)})
		}
	}
	// The host re-applies the plugin configuration several times during
	// startup. Reusing the egress pool keeps its probe results, its parked
	// exits, its warm ring and its latency history, and it keeps the
	// background probe from running once per reconfigure.
	var egress *egressPool
	if previous != nil && previous.egress != nil && previous.egress.reusable(cfg.Egress, cfg.Endpoints) {
		egress = previous.egress
	} else {
		var errEgress error
		egress, errEgress = newEgressPool(cfg.Egress, cfg.Endpoints)
		if errEgress != nil {
			return nil, errEgress
		}
	}
	rt.egress = egress
	rt.health = newHealth()
	hosts := make(map[string]string, len(cfg.Endpoints))
	for i := range cfg.Endpoints {
		ep := &cfg.Endpoints[i]
		if parsed, errParse := url.Parse(ep.URL); errParse == nil {
			hosts[strings.ToLower(parsed.Hostname())] = ep.Egress
		}
	}
	rt.client = newRotatingClient(egress, cfg, hosts)
	return rt, nil
}

func loadedRuntime() *runtime {
	if raw := activeConfig.Load(); raw != nil {
		if rt, ok := raw.(*runtime); ok && rt != nil {
			return rt
		}
	}
	return &runtime{cfg: defaultConfig(), provider: "rotator"}
}

// nextSlot returns the next (endpoint, api key) pair round-robin.
// firstByte is the per-attempt time-to-first-byte budget.
func (r *runtime) firstByte() time.Duration {
	if r == nil || r.cfg.FirstByteSeconds <= 0 {
		return 60 * time.Second
	}
	return time.Duration(r.cfg.FirstByteSeconds) * time.Second
}

func (r *runtime) nextSlot() (slot, bool) {
	if len(r.slots) == 0 {
		return slot{}, false
	}
	idx := r.epCounter.Add(1) - 1
	return r.slots[int(idx%uint64(len(r.slots)))], true
}

// slotsFor returns the rotation order starting at the next slot, filtered by endpoint name.
func (r *runtime) slotsFor(endpointName string) []slot {
	if len(r.slots) == 0 {
		return nil
	}
	start := int((r.epCounter.Add(1) - 1) % uint64(len(r.slots)))
	out := make([]slot, 0, len(r.slots))
	for i := 0; i < len(r.slots); i++ {
		s := r.slots[(start+i)%len(r.slots)]
		if endpointName != "" && !strings.EqualFold(s.endpoint.Name, endpointName) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func (r *runtime) upstreamModel(model string) string {
	for _, m := range r.models {
		if m.ID == model {
			if strings.TrimSpace(m.Upstream) != "" {
				return m.Upstream
			}
			return m.ID
		}
	}
	return model
}

func (r *runtime) endpointForModel(model string) string {
	for _, m := range r.models {
		if m.ID == model {
			return strings.TrimSpace(m.Endpoint)
		}
	}
	return ""
}

func modelResponse() pluginapi.ModelResponse {
	rt := loadedRuntime()
	models := make([]pluginapi.ModelInfo, 0, len(rt.models))
	for _, m := range rt.models {
		name := m.Upstream
		if name == "" {
			name = m.ID
		}
		display := m.DisplayName
		if display == "" {
			display = m.ID
		}
		owned := m.OwnedBy
		if owned == "" {
			owned = rt.provider
		}
		input := m.InputModalities
		if len(input) == 0 {
			input = []string{"text"}
		}
		output := m.OutputModalities
		if len(output) == 0 {
			output = []string{"text"}
		}
		info := pluginapi.ModelInfo{
			ID:                        m.ID,
			Object:                    "model",
			OwnedBy:                   owned,
			Type:                      "openai-compatibility",
			DisplayName:               display,
			Name:                      name,
			Description:               m.Description,
			ContextLength:             m.ContextLength,
			InputTokenLimit:           m.ContextLength,
			MaxCompletionTokens:       m.MaxCompletionTokens,
			OutputTokenLimit:          m.MaxCompletionTokens,
			SupportedParameters:       m.SupportedParameters,
			SupportedInputModalities:  input,
			SupportedOutputModalities: output,
			UserDefined:               true,
		}
		if len(m.ThinkingLevels) > 0 {
			info.Thinking = &pluginapi.ThinkingSupport{Levels: m.ThinkingLevels, ZeroAllowed: true, DynamicAllowed: true}
		}
		models = append(models, info)
	}
	return pluginapi.ModelResponse{Provider: rt.provider, Models: models}
}

// hostAttached records whether the host callback ever worked. Without it the
// plugin runs standalone (unit tests), and a failed host.log is expected.
var hostAttached atomic.Bool

func hostLog(level, message string) {
	payload, errMarshal := json.Marshal(map[string]string{"level": level, "message": message})
	if errMarshal != nil {
		return
	}
	raw, errCall := callHost("host.log", payload)
	if errCall != nil {
		if hostAttached.Load() {
			fmt.Fprintf(os.Stderr, "cliproxy-rotator: host.log failed: %v\n", errCall)
		}
		return
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(raw, &env); errUnmarshal == nil && !env.OK && env.Error != nil {
		if hostAttached.Load() {
			fmt.Fprintf(os.Stderr, "cliproxy-rotator: host.log rejected: %s: %s\n", env.Error.Code, env.Error.Message)
		}
		return
	}
	hostAttached.Store(true)
}

func socketDebug() string {
	if v := os.Getenv("CLIPROXY_ROTATOR_DEBUG"); v != "" {
		return v
	}
	return ""
}

// candidatesFor returns the rotation order of (endpoint, key) slots for a
// model, starting at the round-robin cursor and filtered by the model's
// endpoint binding.
func (r *runtime) candidatesFor(model string) []slot {
	endpoint := r.endpointForModel(model)
	ordered := r.slotsFor(endpoint)
	if len(ordered) == 0 {
		return r.slotsFor("")
	}
	return ordered
}

// takeCandidate returns the next healthy slot from candidates starting at
// cursor, advancing the cursor. When everything is cooling down it returns the
// shortest remaining wait so the caller can tell the agent when to retry.
func (r *runtime) takeCandidate(candidates []slot, cursor *int) (slot, time.Duration, bool) {
	if len(candidates) == 0 {
		return slot{}, 0, false
	}
	wait := time.Duration(0)
	var lastResort slot
	var lastResortWait time.Duration
	haveLastResort := false
	for tries := 0; tries < len(candidates); tries++ {
		index := (*cursor) % len(candidates)
		*cursor = *cursor + 1
		candidate := candidates[index]
		if r.health.usable(slotKey(candidate)) {
			return candidate, 0, true
		}
		remaining := r.health.cooldownRemaining(slotKey(candidate))
		if remaining > wait {
			wait = remaining
		}
		if !haveLastResort || remaining < lastResortWait {
			lastResort = candidate
			lastResortWait = remaining
			haveLastResort = true
		}
	}
	// Everything is cooling down. A parked slot is usually exit-specific (the
	// same upstream works from another exit), so keep trying the closest one
	// instead of failing the request outright.
	if haveLastResort {
		return lastResort, 0, true
	}
	return slot{}, wait, false
}

func contextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}
