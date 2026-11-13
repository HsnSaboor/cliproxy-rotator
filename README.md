# cliproxy-rotator

A **CLIProxyAPI plugin** (no fork, no core patch) that turns any number of
upstream endpoints into one rotating, self-healing provider.

```text
client ──► CLIProxyAPI (:8317) ──► cliproxy-rotator.so ──► exit pool ──► upstream
                    │                                        │
                    └── protocol translation (host) ─────────┴── /48 IPv6 sources, SOCKS5 exits, direct
```

The plugin deliberately does **no** API-format translation. It declares
`executor_input_formats = executor_output_formats = ["chat-completions"]`, so
CLIProxyAPI keeps owning chat-completions ⇄ responses ⇄ messages conversion
while the plugin only decides *which exit* carries *which request*.

## Why it exists

Free upstream tiers (`api.kilo.ai`, NIM, apinex…) rate limit per IP with daily
and hourly windows. One IP is not enough, and a naive "ban the IP forever"
policy throws away the fastest exits for good. This plugin rotates the source
address over a routed IPv6 `/48` (one address per `/64`) and/or a pool of
SOCKS5 exits, and remembers which exits are fast, so:

- the **fastest** exit is used first (latency EWMA + failure penalty);
- an exit that hits a daily/hourly quota is parked for a **bounded** time and
  then automatically rejoins the pool;
- a request never hangs: every attempt, first byte and idle gap has a budget,
  and the whole call is bounded by `total_timeout_seconds`;
- failures surface as `retryable=true; retry_in=Ns` so the calling agent knows
  to try again instead of giving up.

## Capabilities used

| ABI method | Purpose |
| --- | --- |
| `executor.identifier` | Registers the provider id (`rotator`). |
| `executor.execute` / `execute_stream` | Performs the upstream call through a chosen exit. |
| `executor.count_tokens` | Local estimate so the host can size context. |
| `model.register` + `model.static` | Publishes the plugin-owned model ids and their limits/modalities. |
| `model.route` | Claims its own models → `ModelRouteTargetSelf` (otherwise the host answers `auth_not_found`). |
| `plugin.reconfigure` | Applies a new config without a restart. |

## Configuration

Add this to `~/.cli-proxy-api/config.yaml` (the real config already has it):

```yaml
plugins:
  enabled: true
  dir: /home/saboor/.cache/cliproxy-plugins
  configs:
    cliproxy-rotator:
      enabled: true
      priority: 50
      provider: rotator
      max_attempts: 8
      total_timeout_seconds: 180
      first_byte_timeout_seconds: 60
      idle_timeout_seconds: 120
      endpoints:
        - name: kilo
          url: https://api.kilo.ai/api/gateway/v1
          api_keys: [""]       # "" = anonymous, or list real keys
          # egress: auto       # per-endpoint override: auto|ipv6|socks|direct
      egress:
        mode: auto             # auto = IPv6 when the host has AAAA, else SOCKS, else direct
        prefixes: [2001:470:52fa::/48]
        interface: he-ipv6
        pool_size: 2048
        socks_file: /home/saboor/.cache/zen-egress/socks.txt
      models:
        - id: rotator/particle-1-free
          upstream: "particle-ai/particle-1:free"
          context_length: 1048576
          max_completion_tokens: 131072
          input_modalities: ["text", "image"]
```

`tools/generate_kilo_config.py` writes both this block and the matching
`openai-compatibility` provider from the live Kilo model list:

```bash
python3 tools/generate_kilo_config.py --format plugin   # cliproxy-rotator models
python3 tools/generate_kilo_config.py --format snippet  # openai-compatibility provider
```

## Egress behaviour

| Policy | Behaviour |
| --- | --- |
| `ipv6` | Binds a source address from the routed prefix. Used for hosts with AAAA records. |
| `socks` | Round-robins the SOCKS5 exits listed in `socks_file` (produced by `zen-egress-pool`). |
| `direct` | No rotation (control path). |
| `auto` | Probes the host's AAAA once (cached 10 min): IPv6 if v6-reachable, else SOCKS, else direct. |

Address assignment and exit discovery live outside the plugin:

```bash
sudo /usr/local/bin/he-ipv6-pool.sh --status     # pool_size=512
sudo systemctl status he-ipv6.service            # HE tunnel + routed /48
systemctl --user status zen-egress-pool.service  # SOCKS5 exits -> socks.txt
```

### Bundled warm pool (local, not committed)

Proxy lists are data with a short lifetime (and a subscription identity), so
they are **not** in this repository. `make install` copies whatever is in the
local `proxies/` directory next to the plugin, and `egress.socks_fallback`
points there: it is only read when `egress.socks_file` is missing or yields no
usable exit, so the live pool always wins.

Generate the snapshot yourself:

```bash
mkdir -p proxies
zen-egress-pool --once --dump-candidates proxies/candidates.txt \
  --profile ~/path/to/sing-box-profile.json \
  --extra-list ~/path/to/proxies.txt \
  --verify-url https://api.kilo.ai/api/gateway/models \
  --out proxies/socks.txt --state /tmp/zen-state.json
```

## Companion repository

The exit pool that feeds `egress.socks_file` is
[zen-egress-pool](https://github.com/HsnSaboor/zen-egress-pool): it merges
Hiddify/sing-box profiles with omnibot and ytapi proxy lists, probes every exit,
verifies the best ones against the real upstream and writes the ranked list.

## Timing model

| Knob | Default | Meaning |
| --- | --- | --- |
| `max_attempts` | 6–8 | Attempts across exits/keys before giving up. |
| `total_timeout_seconds` | 120–180 | Hard wall for one client call. |
| `first_byte_timeout_seconds` | 60 | Time to headers. |
| `idle_timeout_seconds` | 120 | Max gap between stream chunks; on expiry an error chunk is emitted. |

Streaming uses no context deadline (that would cut the body mid-stream);
instead the pump enforces first-byte and idle budgets and always terminates the
client stream, either with data + `[DONE]` or with a retryable error chunk.

## Health and recovery

- Per-slot exponential backoff `2s → 128s`, capped at 2 min (`health.go`).
- Per-exit cooldowns (`parkFor`) bounded at 5 min; a 429 parks the *exit*, not
  the endpoint, so the next request simply uses another exit.
- Exits are re-probed by the pool every 5 min; the list is hot-reloaded when
  `socks_file` changes (mtime check every 60 s).
- Keep-alives: `MaxIdleConnsPerHost: 8`, `IdleConnTimeout: 90s` per exit, so
  ~30 concurrent requests reuse warm connections instead of re-dialling.

## Build, test, install

```bash
make build     # CGO_ENABLED=1 go build -buildmode=c-shared -o cliproxy-rotator.so .
make test      # go test ./...   (decision-logic tests, no network)
make install   # copies the .so into ~/.cache/cliproxy-plugins
make reload    # systemctl --user restart cli-proxy-api.service
```

The plugin must be built against the same CLIProxyAPI source the server was
built from; `go.mod` points at `../CLIProxyAPI` (stock upstream checkout).

## Verified behaviour

- `POST /v1/chat/completions` (non-stream) → 200 through a rotating exit.
- `stream: true` → full chunk stream, `finish_reason` and `[DONE]`.
- `/v1/messages` (Claude protocol) → 200, translated by the host, not the plugin.
- 15 concurrent requests → 15 × 200 in 4.7 s.
- Unreachable endpoint → fails in <1 s with `retryable=true` and the attempt log.
- `api.kilo.ai` (IPv4-only) → automatically uses the SOCKS pool.
- `api.apinex.bond` (AAAA) with `egress: ipv6` → dials from the `/48` pool.
