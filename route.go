package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type rpcModelRouteRequest struct {
	pluginapi.ModelRouteRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

// routeModel claims every request whose model this plugin publishes, so the
// host runs it through our executor instead of looking for a credential.
func routeModel(raw []byte) ([]byte, error) {
	var req rpcModelRouteRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, fmt.Errorf("decode model route request: %w", errUnmarshal)
	}
	rt := loadedRuntime()
	if rt.cfg.Enabled != nil && !*rt.cfg.Enabled {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false, Reason: "plugin disabled"})
	}
	model := strings.TrimSpace(req.RequestedModel)
	if model == "" {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	if !rt.ownsModel(model) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	return okEnvelope(pluginapi.ModelRouteResponse{
		Handled:    true,
		TargetKind: pluginapi.ModelRouteTargetSelf,
		Reason:     "cliproxy-rotator owns " + model,
	})
}

// ownsModel reports whether model (bare or provider-prefixed) is ours.
func (r *runtime) ownsModel(model string) bool {
	candidates := []string{model, strings.TrimPrefix(model, r.provider+"/"), strings.TrimPrefix(model, r.provider+"_")}
	for _, candidate := range candidates {
		for _, m := range r.models {
			if m.ID == candidate {
				return true
			}
		}
	}
	return false
}
