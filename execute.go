package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type rpcExecutorRequest struct {
	pluginapi.ExecutorRequest
	StreamID       string `json:"stream_id,omitempty"`
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcExecutorHTTPRequest struct {
	pluginapi.ExecutorHTTPRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

// upstreamPath maps a CLIProxyAPI protocol format to its HTTP path on an
// OpenAI-compatible endpoint. The host does the format translation; this only
// picks the wire path, and chat-completions is the format we declare.
func upstreamPath(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "chat-completions", "chat_completions", "openai", "":
		return "/chat/completions"
	case "responses", "openai-response":
		return "/responses"
	case "claude", "anthropic", "messages":
		return "/messages"
	case "embeddings":
		return "/embeddings"
	default:
		return "/chat/completions"
	}
}

// rewriteModel replaces the payload's model field with the upstream model
// name. Everything else in the payload is preserved byte-for-byte, so prompt
// caching fields and provider extras survive untouched.
func rewriteModel(payload []byte, model string) []byte {
	if len(payload) == 0 || model == "" {
		return payload
	}
	var doc map[string]json.RawMessage
	if errUnmarshal := json.Unmarshal(payload, &doc); errUnmarshal != nil {
		return payload
	}
	encoded, errMarshal := json.Marshal(model)
	if errMarshal != nil {
		return payload
	}
	current, ok := doc["model"]
	if ok && bytes.Equal(current, encoded) {
		return payload
	}
	doc["model"] = encoded
	out, errMarshalAll := json.Marshal(doc)
	if errMarshalAll != nil {
		return payload
	}
	return out
}

type upstreamCall struct {
	url     string
	headers http.Header
	body    []byte
	label   string
}

func (r *runtime) prepare(slot slot, req *rpcExecutorRequest, stream bool) *upstreamCall {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	if stream {
		headers.Set("Accept", "text/event-stream")
	} else {
		headers.Set("Accept", "application/json")
	}
	userAgent := r.cfg.UserAgent
	if userAgent == "" && req.Headers != nil {
		if existing := req.Headers.Get("User-Agent"); existing != "" && !strings.Contains(existing, "cliproxy") {
			userAgent = existing
		}
	}
	if userAgent == "" {
		userAgent = "cliproxy-rotator/1.0"
	}
	headers.Set("User-Agent", userAgent)
	if strings.HasSuffix(upstreamPath(req.Format), "/messages") {
		// Anthropic-protocol upstreams want a version header; forward the
		// client's own when present, otherwise state the current one.
		if existing := req.Headers.Get("Anthropic-Version"); existing != "" {
			headers.Set("Anthropic-Version", existing)
		} else {
			headers.Set("Anthropic-Version", "2023-06-01")
		}
	}
	if slot.apiKey != "" {
		headers.Set("Authorization", "Bearer "+slot.apiKey)
	}
	for name, value := range slot.endpoint.Headers {
		headers.Set(name, value)
	}
	body := rewriteModel(req.Payload, r.upstreamModel(req.Model))
	return &upstreamCall{
		url:     slot.endpoint.URL + upstreamPath(req.Format),
		headers: headers,
		body:    body,
		label:   fmt.Sprintf("%s key#%s", slot.endpoint.Name, maskKey(slot.apiKey)),
	}
}

func maskKey(key string) string {
	if key == "" {
		return "anon"
	}
	if len(key) <= 8 {
		return "***"
	}
	return key[:4] + "..." + key[len(key)-4:]
}

func (r *runtime) timeout() time.Duration {
	if r.cfg.TotalTimeoutSeconds > 0 {
		return time.Duration(r.cfg.TotalTimeoutSeconds) * time.Second
	}
	return 120 * time.Second
}

// hedgeDelay is how long a request may wait for response headers before a
// second exit is raced. Negative disables hedging.
func (r *runtime) hedgeDelay() time.Duration {
	ms := r.cfg.HedgeDelayMs
	if ms < 0 {
		return 0
	}
	if ms == 0 {
		ms = 550
	}
	return time.Duration(ms) * time.Millisecond
}

// send performs one upstream attempt through the best exit for the endpoint's
// host, reporting which exit was used so caller can park it on 429s. When
// hedging is enabled a second exit is raced if the first is slow to answer.
func (r *runtime) send(call *upstreamCall, timeout time.Duration, stream bool) (*http.Response, *egressEntry, error) {
	host, policy := r.routeFor(call.url)
	entry, cooldown := r.client.pool.pick(host, policy)
	if entry == nil {
		if cooldown > 0 {
			return nil, nil, newParkedError("all exits for "+host+" are cooling down", cooldown)
		}
		return nil, nil, fmt.Errorf("no egress available for %s", host)
	}
	if timeout <= 0 {
		timeout = r.timeout()
	}
	if timeout <= 0 {
		return nil, entry, fmt.Errorf("invalid timeout")
	}
	if hedge := r.hedgeDelay(); hedge > 0 {
		return r.sendHedged(call, host, policy, entry, timeout, stream, hedge)
	}
	return r.attemptOnce(call, entry, policy, timeout, stream)
}

// routeFor resolves the egress policy configured for a URL's host.
func (r *runtime) routeFor(rawURL string) (string, string) {
	parsed, errParse := url.Parse(rawURL)
	host := ""
	if errParse == nil {
		host = strings.ToLower(parsed.Hostname())
	}
	policy := policyAuto
	if r.client != nil {
		if p, ok := r.client.policy[host]; ok {
			policy = p
		}
	}
	return host, policy
}

// attemptOnce runs one attempt pinned to one exit.
func (r *runtime) attemptOnce(call *upstreamCall, entry *egressEntry, policy string, timeout time.Duration, stream bool) (*http.Response, *egressEntry, error) {
	request, errNew := http.NewRequest(http.MethodPost, call.url, bytes.NewReader(call.body))
	if errNew != nil {
		return nil, entry, errNew
	}
	request.Header = call.headers.Clone()
	ctx, cancel := context.WithCancel(context.Background())
	if !stream {
		// Non-streaming calls are fully bounded by the request budget.
		ctx, cancel = contextWithTimeout(timeout)
	}
	request = request.WithContext(ctx)
	client := &http.Client{Transport: &singleEntryTransport{entry: entry}, Timeout: 0}
	started := time.Now()
	entry.acquire()
	resp, errDo := client.Do(request)
	entry.release()
	if errDo != nil {
		cancel()
		if unreachableExit(errDo) {
			// The exit itself is gone: hold it out for a few minutes
			// instead of paying the dial timeout again on every call.
			entry.parkDead(errDo.Error())
		} else {
			entry.parkFor(errDo.Error(), false)
		}
		return nil, entry, errDo
	}
	entry.recordLatency(time.Since(started))
	entry.succeed()
	// The context must outlive the call for a stream: cancel on body close.
	resp.Body = cancelBody{ReadCloser: resp.Body, cancel: cancel}
	if socketDebug() != "" {
		hostLog("debug", "cliproxy-rotator: egress "+entry.label+" ("+policy+") -> "+call.url)
	}
	return resp, entry, nil
}

// sendHedged races a second exit when the first has not produced headers
// within the hedge delay. The first acceptable answer wins and the other
// attempt is cancelled, so a slow exit costs a few hundred milliseconds
// instead of the whole first-byte budget.
func (r *runtime) sendHedged(call *upstreamCall, host, policy string, first *egressEntry, timeout time.Duration, stream bool, hedge time.Duration) (*http.Response, *egressEntry, error) {
	type outcome struct {
		resp   *http.Response
		entry  *egressEntry
		err    error
		handle *attemptHandle
	}
	results := make(chan outcome, 4)
	launched := map[string]bool{}
	var handles []*attemptHandle
	cancelAll := func() {
		for _, handle := range handles {
			if handle != nil && handle.cancel != nil {
				handle.cancel()
			}
		}
	}
	launch := func(entry *egressEntry) bool {
		if entry == nil || launched[entry.label] {
			return false
		}
		launched[entry.label] = true
		request, errNew := http.NewRequest(http.MethodPost, call.url, bytes.NewReader(call.body))
		if errNew != nil {
			return false
		}
		request.Header = call.headers.Clone()
		ctx, cancel := context.WithCancel(context.Background())
		if !stream {
			ctx, cancel = contextWithTimeout(timeout)
		}
		handle := &attemptHandle{cancel: cancel}
		handles = append(handles, handle)
		request = request.WithContext(ctx)
		client := &http.Client{Transport: &singleEntryTransport{entry: entry}, Timeout: 0}
		go func() {
			started := time.Now()
			entry.acquire()
			resp, errDo := client.Do(request)
			entry.release()
			if errDo == nil {
				entry.recordLatency(time.Since(started))
			}
			results <- outcome{resp: resp, entry: entry, err: errDo, handle: handle}
		}()
		return true
	}

	launch(first)
	hedgeTimer := time.After(hedge)
	var lastErr error
	var lastEntry *egressEntry
	pending := 1
	exhausted := 0
	for pending > 0 {
		select {
		case out := <-results:
			pending--
			if out.err != nil {
				lastErr, lastEntry = out.err, out.entry
				if unreachableExit(out.err) {
					out.entry.parkDead(out.err.Error())
				} else {
					out.entry.parkFor(out.err.Error(), false)
				}
				if exhausted < r.attemptBudget() {
					if next, _ := r.client.pool.pick(host, policy); launch(next) {
						exhausted++
						pending++
					}
				}
				continue
			}
			if out.resp.StatusCode >= 500 {
				// A 5xx is a failed attempt: keep the race going.
				lastErr = fmt.Errorf("http %d", out.resp.StatusCode)
				lastEntry = out.entry
				out.entry.parkFor(lastErr.Error(), false)
				_, _ = io.Copy(io.Discard, out.resp.Body)
				out.resp.Body.Close()
				if out.handle != nil && out.handle.cancel != nil {
					out.handle.cancel()
				}
				if exhausted < r.attemptBudget() {
					if next, _ := r.client.pool.pick(host, policy); launch(next) {
						exhausted++
						pending++
					}
				}
				continue
			}
			// Winner: keep its context alive until the body is closed.
			out.entry.succeed()
			winnerCancel := context.CancelFunc(nil)
			if out.handle != nil {
				winnerCancel = out.handle.cancel
			}
			out.resp.Body = cancelBody{ReadCloser: out.resp.Body, cancel: winnerCancel}
			for _, handle := range handles {
				if handle == nil || handle == out.handle || handle.cancel == nil {
					continue
				}
				handle.cancel() // abort the losing attempt
			}
			if socketDebug() != "" {
				hostLog("debug", "cliproxy-rotator: hedged winner "+out.entry.label+" ("+policy+") -> "+call.url)
			}
			return out.resp, out.entry, nil
		case <-hedgeTimer:
			hedgeTimer = nil
			if next, _ := r.client.pool.pick(host, policy); launch(next) {
				pending++
				exhausted++
			}
		}
	}
	cancelAll()
	if lastErr == nil {
		lastErr = fmt.Errorf("no egress attempt completed for %s", host)
	}
	return nil, lastEntry, lastErr
}

// singleEntryTransport pins one request to one exit (so failures are attributable).
type singleEntryTransport struct{ entry *egressEntry }

func (t *singleEntryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.entry.httpTransport().RoundTrip(req)
}

// slotKey identifies a (endpoint, key) pair for health tracking.
func slotKey(s slot) string {
	return "slot:" + s.endpoint.Name + "|" + s.apiKey
}

// attemptBudget bounds how many upstream tries a request gets. It is
// independent of the slot count: the same endpoint may succeed on a different
// egress exit, so a single anonymous endpoint still gets several tries.
func (r *runtime) attemptBudget() int {
	limit := r.cfg.MaxAttempts
	if limit <= 0 {
		limit = 6
	}
	return limit
}

// execute performs a non-streaming call. It walks the rotation until the time
// budget or attempt budget runs out, then returns a clear, actionable error.
func execute(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, fmt.Errorf("decode executor request: %w", errUnmarshal)
	}
	debugFormat(&req, false)
	rt := loadedRuntime()
	if len(rt.slots) == 0 {
		return nil, fmt.Errorf("cliproxy-rotator: no endpoints configured; add endpoints to the plugin config and restart")
	}
	deadline := time.Now().Add(rt.timeout())
	attempts := rt.attemptBudget()
	report := &failureReport{}
	candidates := rt.candidatesFor(req.Model)
	cursor := 0
	rateLimited := 0
	var retryIn time.Duration
	for i := 0; i < attempts; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			report.add("budget", fmt.Errorf("total_timeout=%ds exhausted after %d attempts", rt.cfg.TotalTimeoutSeconds, i))
			break
		}
		slot, wait, ok := rt.takeCandidate(candidates, &cursor)
		if !ok {
			report.add("selection", fmt.Errorf("all endpoints cooling down; retry_in=%.0fs", wait.Seconds()))
			break
		}
		call := rt.prepare(slot, &req, false)
		// One slow exit must not eat the whole budget: cap a single non-stream
		// attempt so the next (faster) exit is tried quickly.
		attemptCap := rt.firstByte()
		if attemptCap <= 0 || attemptCap > remaining {
			attemptCap = remaining
		}
		resp, usedExit, errSend := rt.send(call, attemptCap, false)
		if errSend != nil {
			rt.health.fail(slotKey(slot), errSend.Error())
			report.add(call.label, errSend)
			if !retryableNetworkError(errSend) {
				break
			}
			continue
		}
		body, errRead := io.ReadAll(resp.Body)
		resp.Body.Close()
		if errRead != nil {
			rt.health.fail(slotKey(slot), errRead.Error())
			report.add(call.label, errRead)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			rt.health.succeed(slotKey(slot))
			return okEnvelope(pluginapi.ExecutorResponse{Payload: body, Headers: flatHeaders(resp.Header)})
		}
		rt.health.fail(slotKey(slot), fmt.Sprintf("http %d", resp.StatusCode))
		report.add(call.label, fmt.Errorf("http %d", resp.StatusCode))
		if rateLimitedStatus(resp.StatusCode) {
			rateLimited++
			if usedExit != nil {
				usedExit.parkFor("upstream http 429 (rate limited)", true)
			}
			if wait := rateLimitCooldown(usedExit); wait > retryIn {
				retryIn = wait
			}
			// Three exits throttled for the same request means the model is
			// throttled, not the path: fail fast with a retry hint instead of
			// burning the rest of the attempt budget.
			if rateLimited >= 3 {
				return nil, fmt.Errorf("cliproxy-rotator: upstream is rate limiting this model on %d exits; retryable=true; retry_in=%.0fs; %s",
					rateLimited, retryIn.Seconds(), report.String())
			}
		}
		if !retryableStatus(resp.StatusCode) {
			return nil, newStatusError(resp.StatusCode, fmt.Sprintf(
				"upstream %s rejected the request (http %d); retryable=false; body: %s",
				call.label, resp.StatusCode, clipBody(body)))
		}
	}
	return nil, fmt.Errorf("cliproxy-rotator: request failed after %d attempts; retryable=true; retry_in=%.0fs; %s", attempts, retryIn.Seconds(), report.String())
}

// executeStream starts the upstream stream in the background with strict
// first-byte and idle budgets, emitting errors through the stream bridge so a
// hanging upstream can never leave the client waiting forever.
func executeStream(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, fmt.Errorf("decode executor stream request: %w", errUnmarshal)
	}
	debugFormat(&req, true)
	rt := loadedRuntime()
	if len(rt.slots) == 0 {
		return nil, fmt.Errorf("cliproxy-rotator: no endpoints configured; add endpoints to the plugin config and restart")
	}
	firstByte := time.Duration(rt.cfg.FirstByteSeconds) * time.Second
	if firstByte <= 0 {
		firstByte = 60 * time.Second
	}
	deadline := time.Now().Add(firstByte)
	attempts := rt.attemptBudget()
	report := &failureReport{}
	candidates := rt.candidatesFor(req.Model)
	cursor := 0
	var resp *http.Response
	var call *upstreamCall
	rateLimited := 0
	var retryIn time.Duration
	for i := 0; i < attempts; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			report.add("budget", fmt.Errorf("first_byte_timeout=%ds exhausted after %d attempts", int(firstByte.Seconds()), i))
			break
		}
		slot, wait, ok := rt.takeCandidate(candidates, &cursor)
		if !ok {
			report.add("selection", fmt.Errorf("all endpoints cooling down; retry_in=%.0fs", wait.Seconds()))
			break
		}
		call = rt.prepare(slot, &req, true)
		attempt, usedExit, errSend := rt.send(call, remaining, true)
		if errSend != nil {
			rt.health.fail(slotKey(slot), errSend.Error())
			report.add(call.label, errSend)
			if !retryableNetworkError(errSend) {
				break
			}
			continue
		}
		if attempt.StatusCode >= 200 && attempt.StatusCode < 300 {
			resp = attempt
			rt.health.succeed(slotKey(slot))
			break
		}
		body, _ := io.ReadAll(attempt.Body)
		attempt.Body.Close()
		rt.health.fail(slotKey(slot), fmt.Sprintf("http %d", attempt.StatusCode))
		report.add(call.label, fmt.Errorf("http %d", attempt.StatusCode))
		if rateLimitedStatus(attempt.StatusCode) {
			rateLimited++
			if usedExit != nil {
				usedExit.parkFor("upstream http 429 (rate limited)", true)
			}
			if wait := rateLimitCooldown(usedExit); wait > retryIn {
				retryIn = wait
			}
			if rateLimited >= 3 {
				return nil, fmt.Errorf("cliproxy-rotator: upstream is rate limiting this model on %d exits; retryable=true; retry_in=%.0fs; %s",
					rateLimited, retryIn.Seconds(), report.String())
			}
		}
		if !retryableStatus(attempt.StatusCode) {
			return nil, newStatusError(attempt.StatusCode, fmt.Sprintf(
				"upstream %s rejected the request (http %d); retryable=false; body: %s",
				call.label, attempt.StatusCode, clipBody(body)))
		}
	}
	if resp == nil {
		return nil, fmt.Errorf("cliproxy-rotator: stream failed after %d attempts; retryable=true; retry_in=%.0fs; %s", attempts, retryIn.Seconds(), report.String())
	}
	streamID := req.StreamID
	idle := time.Duration(rt.cfg.IdleSeconds) * time.Second
	if idle <= 0 {
		idle = 120 * time.Second
	}
	// A stream that dies before delivering a single byte is retried on a fresh
	// exit. Reopening stops as soon as any byte reached the client, because a
	// second response would duplicate text.
	reopenBudget := rt.attemptBudget()
	reopen := func() (*http.Response, string, error) {
		for reopenBudget > 0 {
			reopenBudget--
			slotNext, _, okNext := rt.takeCandidate(candidates, &cursor)
			if !okNext {
				return nil, "", fmt.Errorf("all endpoints are cooling down")
			}
			nextCall := rt.prepare(slotNext, &req, true)
			r, _, errSend := rt.send(nextCall, rt.timeout(), true)
			if errSend != nil {
				rt.health.fail(slotKey(slotNext), errSend.Error())
				continue
			}
			if r.StatusCode >= 200 && r.StatusCode < 300 {
				return r, nextCall.label, nil
			}
			rt.health.fail(slotKey(slotNext), fmt.Sprintf("http %d", r.StatusCode))
			_, _ = io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
		return nil, "", fmt.Errorf("retry budget exhausted")
	}
	go pumpStream(streamID, resp, call.label, idle, reopen)
	return okEnvelope(map[string]any{})
}

// pumpStream copies upstream bytes into host stream chunks, enforcing an idle
// budget: if no chunk arrives for `idle`, the stream is closed with an error
// chunk so the agent sees "retry" instead of a hanging connection. If the exit
// dies before delivering a single byte (no terminal event either), the request
// is transparently reopened on a fresh exit: nothing was emitted yet, so the
// client cannot see the retry.
func pumpStream(streamID string, resp *http.Response, label string, idle time.Duration, reopen func() (*http.Response, string, error)) {
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()
	if streamID == "" {
		_, _ = io.ReadAll(resp.Body)
		return
	}
	for reopens := 0; ; reopens++ {
		terminal, emitted, errText := pumpOnce(streamID, resp, label, idle)
		resp.Body.Close()
		if terminal || emitted > 0 || reopen == nil {
			closeStream(streamID, errText)
			return
		}
		next, nextLabel, errReopen := reopen()
		if errReopen != nil {
			// Nothing usable left: tell the client to retry rather than
			// ending the turn with an empty stream.
			closeStream(streamID, fmt.Sprintf("stream from %s closed before any data and no exit was left to retry (%s); retryable=true; retry_in=2s", label, errReopen.Error()))
			return
		}
		hostLog("debug", fmt.Sprintf("cliproxy-rotator: %s closed with no data, retried on %s", label, nextLabel))
		resp, label = next, nextLabel
	}
}

// terminalEvent reports whether one SSE event block ends the stream.
func terminalEvent(block []byte) bool {
	if bytes.Contains(block, []byte("[DONE]")) {
		return true
	}
	for _, marker := range []string{
		`"type":"message_stop"`,
		`"type":"response.completed"`,
		`"type":"response.failed"`,
		`"type":"response.incomplete"`,
		`"type":"response.done"`,
	} {
		if bytes.Contains(block, []byte(marker)) {
			return true
		}
	}
	// chat-completions finishes with a non-null finish_reason.
	if bytes.Contains(block, []byte(`"finish_reason":"`)) {
		return true
	}
	return false
}

// pumpOnce forwards one response body. It reports whether a terminal event was
// seen, how many bytes reached the client, and an error text to close with when
// the stream broke after real data had already been delivered.
func pumpOnce(streamID string, resp *http.Response, label string, idle time.Duration) (bool, int, string) {
	buffer := make([]byte, 32*1024)
	// Emit exactly one SSE event per host callback. The host's responses-stream
	// translator consumes a single event from each emitted chunk, so batching
	// several frames into one call silently drops all but the first.
	pending := make([]byte, 0, 64*1024)
	terminal := false
	emitted := 0
	flush := func(final bool) error {
		for len(pending) > 0 {
			idx := bytes.Index(pending, []byte("\n\n"))
			cut := 0
			if idx >= 0 {
				cut = idx + 2
			} else if final {
				cut = len(pending)
			} else {
				return nil
			}
			block := pending[:cut]
			if terminalEvent(block) {
				terminal = true
			}
			if errEmit := emitChunk(streamID, block); errEmit != nil {
				return errEmit
			}
			emitted += cut
			pending = append(pending[:0], pending[cut:]...)
			if !final && idx < 0 {
				return nil
			}
		}
		return nil
	}
	timer := time.NewTimer(idle)
	defer timer.Stop()
	for {
		timer.Reset(idle)
		type readResult struct {
			n   int
			err error
		}
		resultChannel := make(chan readResult, 1)
		go func() {
			n, errRead := resp.Body.Read(buffer)
			resultChannel <- readResult{n: n, err: errRead}
		}()
		select {
		case result := <-resultChannel:
			if result.n > 0 {
				pending = append(pending, buffer[:result.n]...)
				if errEmit := flush(false); errEmit != nil {
					hostLog("warn", "cliproxy-rotator: emit failed: "+errEmit.Error())
					return terminal, emitted, ""
				}
			}
			if result.err != nil {
				// Deliver any trailing bytes that never got their newline.
				if errEmit := flush(true); errEmit != nil {
					hostLog("warn", "cliproxy-rotator: emit failed: "+errEmit.Error())
					return terminal, emitted, ""
				}
				if result.err != io.EOF && !isBenignClose(result.err) {
					if emitted == 0 {
						return terminal, 0, ""
					}
					return terminal, emitted, fmt.Sprintf("stream from %s broke: %v; retryable=true; retry_in=2s", label, result.err)
				}
				return terminal, emitted, ""
			}
		case <-timer.C:
			if emitted == 0 {
				return terminal, 0, ""
			}
			return terminal, emitted, fmt.Sprintf("no data from %s for %s; retryable=true; retry_in=2s", label, idle.Round(time.Second))
		}
	}
}

func isBenignClose(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "eof") || strings.Contains(text, "connection closed by foreign host") ||
		strings.Contains(text, "broken pipe") || strings.Contains(text, "context canceled")
}

func clipBody(body []byte) string {
	text := string(body)
	if len(text) > 400 {
		text = text[:400] + "…"
	}
	text = strings.ReplaceAll(text, "\n", " ")
	return text
}

// httpRequestResult serves the executor.http_request bridge (not used by this
// executor's path; kept for ABI completeness).
func httpRequestResult(raw []byte) ([]byte, error) {
	var req rpcExecutorHTTPRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, fmt.Errorf("decode http request: %w", errUnmarshal)
	}
	rt := loadedRuntime()
	if rt.client == nil {
		return nil, fmt.Errorf("cliproxy-rotator: not configured")
	}
	return nil, fmt.Errorf("cliproxy-rotator: executor.http_request is not supported")
}

func flatHeaders(headers http.Header) http.Header {
	names := make([]string, 0, len(headers))
	for name := range headers {
		if strings.EqualFold(name, "content-encoding") || strings.EqualFold(name, "content-length") ||
			strings.EqualFold(name, "transfer-encoding") || strings.EqualFold(name, "connection") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	out := make(http.Header, len(names))
	for _, name := range names {
		out[name] = headers[name]
	}
	return out
}

// debugFormat logs the protocol formats the host handed to the executor so a
// mis-declared capability (which would force translation) is visible.
func debugFormat(req *rpcExecutorRequest, stream bool) {
	if os.Getenv("ROTATOR_DEBUG_FMT") == "" {
		return
	}
	fmt.Fprintf(os.Stderr, "cliproxy-rotator: format=%q source=%q stream=%v path=%s model=%s\n",
		req.Format, req.SourceFormat, stream, upstreamPath(req.Format), req.Model)
}
