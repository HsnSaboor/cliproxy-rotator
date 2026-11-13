package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// send performs one upstream attempt through the best exit for the endpoint's
// host, reporting which exit was used so caller can park it on 429s.
func (r *runtime) send(call *upstreamCall, timeout time.Duration, stream bool) (*http.Response, *egressEntry, error) {
	request, errNew := http.NewRequest(http.MethodPost, call.url, bytes.NewReader(call.body))
	if errNew != nil {
		return nil, nil, errNew
	}
	request.Header = call.headers.Clone()
	host := strings.ToLower(request.URL.Hostname())
	policy := policyAuto
	if r.client != nil {
		if p, ok := r.client.policy[host]; ok {
			policy = p
		}
	}
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
	if timeout > 0 {
		client := &http.Client{Transport: &singleEntryTransport{entry: entry}, Timeout: 0}
		if !stream {
			// Non-streaming calls are fully bounded by the request budget.
			ctx, cancel := contextWithTimeout(timeout)
			defer cancel()
			request = request.WithContext(ctx)
		}
		started := time.Now()
		entry.acquire()
		resp, errDo := client.Do(request)
		entry.release()
		if errDo != nil {
			entry.parkFor(errDo.Error(), false)
			return nil, entry, errDo
		}
		entry.recordLatency(time.Since(started))
		entry.succeed()
		if socketDebug() != "" {
			hostLog("debug", "cliproxy-rotator: egress "+entry.label+" ("+policy+") -> "+call.url)
		}
		return resp, entry, nil
	}
	return nil, entry, fmt.Errorf("invalid timeout")
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
	rt := loadedRuntime()
	if len(rt.slots) == 0 {
		return nil, fmt.Errorf("cliproxy-rotator: no endpoints configured; add endpoints to the plugin config and restart")
	}
	deadline := time.Now().Add(rt.timeout())
	attempts := rt.attemptBudget()
	report := &failureReport{}
	candidates := rt.candidatesFor(req.Model)
	cursor := 0
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
		if rateLimitedStatus(resp.StatusCode) && usedExit != nil {
			usedExit.parkFor("upstream http 429 (rate limited)", true)
		}
		if !retryableStatus(resp.StatusCode) {
			return nil, newStatusError(resp.StatusCode, fmt.Sprintf(
				"upstream %s rejected the request (http %d); retryable=false; body: %s",
				call.label, resp.StatusCode, clipBody(body)))
		}
	}
	return nil, fmt.Errorf("cliproxy-rotator: request failed after %d attempts; %s; retryable=true", attempts, report.String())
}

// executeStream starts the upstream stream in the background with strict
// first-byte and idle budgets, emitting errors through the stream bridge so a
// hanging upstream can never leave the client waiting forever.
func executeStream(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, fmt.Errorf("decode executor stream request: %w", errUnmarshal)
	}
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
		if rateLimitedStatus(attempt.StatusCode) && usedExit != nil {
			usedExit.parkFor("upstream http 429 (rate limited)", true)
		}
		if !retryableStatus(attempt.StatusCode) {
			return nil, newStatusError(attempt.StatusCode, fmt.Sprintf(
				"upstream %s rejected the request (http %d); retryable=false; body: %s",
				call.label, attempt.StatusCode, clipBody(body)))
		}
	}
	if resp == nil {
		return nil, fmt.Errorf("cliproxy-rotator: stream failed after %d attempts; %s; retryable=true", attempts, report.String())
	}
	streamID := req.StreamID
	idle := time.Duration(rt.cfg.IdleSeconds) * time.Second
	if idle <= 0 {
		idle = 120 * time.Second
	}
	go pumpStream(streamID, resp, call.label, idle)
	return okEnvelope(map[string]any{})
}

// pumpStream copies upstream bytes into host stream chunks, enforcing an idle
// budget: if no chunk arrives for `idle`, the stream is closed with an error
// chunk so the agent sees "retry" instead of a hanging connection.
func pumpStream(streamID string, resp *http.Response, label string, idle time.Duration) {
	defer resp.Body.Close()
	if streamID == "" {
		_, _ = io.ReadAll(resp.Body)
		return
	}
	buffer := make([]byte, 32*1024)
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
				if errEmit := emitChunk(streamID, buffer[:result.n]); errEmit != nil {
					hostLog("warn", "cliproxy-rotator: emit failed: "+errEmit.Error())
					return
				}
			}
			if result.err != nil {
				if result.err != io.EOF && !isBenignClose(result.err) {
					closeStream(streamID, fmt.Sprintf("stream from %s broke: %v; retryable=true; retry_in=2s", label, result.err))
				} else {
					closeStream(streamID, "")
				}
				return
			}
		case <-timer.C:
			closeStream(streamID, fmt.Sprintf("no data from %s for %s; retryable=true; retry_in=2s", label, idle.Round(time.Second)))
			return
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
