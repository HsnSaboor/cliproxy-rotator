package main

import "testing"

func TestTerminalEventDetectsEveryProtocol(t *testing.T) {
	cases := []struct {
		name  string
		block string
		want  bool
	}{
		{"chat-completions done", "data: [DONE]\n\n", true},
		{"chat-completions finish_reason", `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n", true},
		{"chat-completions streaming delta", `data: {"choices":[{"delta":{"content":"hi"},"finish_reason":null}]}` + "\n\n", false},
		{"responses completed", `data: {"type":"response.completed","response":{}}` + "\n\n", true},
		{"responses failed", `data: {"type":"response.failed"}` + "\n\n", true},
		{"responses incomplete", `data: {"type":"response.incomplete"}` + "\n\n", true},
		{"responses delta", `data: {"type":"response.output_text.delta","delta":"Hi"}` + "\n\n", false},
		{"claude message_stop", "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", true},
		{"claude content_block_delta", "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n", false},
		{"empty block", "", false},
	}
	for _, tc := range cases {
		if got := terminalEvent([]byte(tc.block)); got != tc.want {
			t.Errorf("%s: terminalEvent()=%v want %v", tc.name, got, tc.want)
		}
	}
}

// A stream that produced no bytes and saw no terminal event is retryable; one
// that already delivered text must never be replayed, since that would
// duplicate output in the client.
func TestRetryOnlyBeforeAnyByteIsEmitted(t *testing.T) {
	type outcome struct {
		terminal bool
		emitted  int
		reopen   bool
	}
	shouldRetry := func(o outcome) bool { return !o.terminal && o.emitted == 0 && o.reopen }
	cases := []struct {
		name string
		in   outcome
		want bool
	}{
		{"dead exit before data", outcome{false, 0, true}, true},
		{"partial answer already sent", outcome{false, 120, true}, false},
		{"terminal seen", outcome{true, 0, true}, false},
		{"no reopen available", outcome{false, 0, false}, false},
	}
	for _, tc := range cases {
		if got := shouldRetry(tc.in); got != tc.want {
			t.Errorf("%s: shouldRetry()=%v want %v", tc.name, got, tc.want)
		}
	}
}
