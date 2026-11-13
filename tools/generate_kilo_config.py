#!/usr/bin/env python3
"""Generate CLIProxyAPI config blocks for Kilo Code's free gateway models.

Kilo's gateway (https://api.kilo.ai/api/gateway) answers /models publicly and
serves its `:free` models anonymously, so a provider block needs no key. Each
model keeps its real context window, completion cap and modalities.

    python3 generate_kilo_config.py --format snippet    # openai-compatibility block
    python3 generate_kilo_config.py --format plugin     # cliproxy-rotator models
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request

MODELS_URL = "https://api.kilo.ai/api/gateway/models"
BASE_URL = "https://api.kilo.ai/api/gateway/v1"

#: Friendly aliases so agents do not have to type nested "provider/model:free".
ALIASES = {
    "particle-ai/particle-1:free": "particle-1-free",
    "kilo-auto/free": "auto-free",
    "baidu/ernie-5.5-flash-preview:free": "ernie-5.5-flash-free",
    "cohere/north-mini-code:free": "north-mini-code-free",
    "dots-studio/dots-3-note-preview:free": "dots-3-note-free",
    "inclusionai/ling-3.2-flash:free": "ling-3.2-flash-free",
    "liquid/lfm-3-1.7b:free": "lfm-3-1.7b-free",
    "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free": "nemotron-3-nano-omni-free",
    "nvidia/nemotron-3-ultra-550b-a55b:free": "nemotron-3-ultra-free",
    "nvidia/nemotron-3.5-lightning:free": "nemotron-3.5-lightning-free",
    "nvidia/nemotron-3.5-super-vl:free": "nemotron-3.5-super-vl-free",
    "openrouter/free": "openrouter-free",
    "reflection/beam-free": "beam-free",
    "stealth/glyph-cluster": "glyph-cluster-free",
}


def fetch_free_models() -> list[dict]:
    with urllib.request.urlopen(MODELS_URL, timeout=60) as response:
        payload = json.load(response)
    models = []
    for model in payload.get("data", []):
        if not model.get("isFree"):
            continue
        arch = model.get("architecture") or {}
        top = model.get("top_provider") or {}
        models.append(
            {
                "id": model["id"],
                "display": model.get("name") or model["id"],
                "context": model.get("context_length") or top.get("context_length") or 0,
                "max_out": top.get("max_completion_tokens") or 0,
                "input": arch.get("input_modalities") or ["text"],
                "output": arch.get("output_modalities") or ["text"],
                "params": model.get("supported_parameters") or [],
                "description": (model.get("description") or "").split("\n")[0][:200],
            }
        )
    models.sort(key=lambda m: (-m["context"], m["id"]))
    return models


def snippet(models: list[dict], provider: str, prefix: str) -> str:
    lines = [f"  - name: {provider}", f"    prefix: {prefix}", f"    base-url: {BASE_URL}", "    models:"]
    for model in models:
        alias = ALIASES.get(model["id"], model["id"].replace("/", "-").replace(":", "-"))
        lines.append(f"      - name: {json.dumps(model['id'])}")
        lines.append(f"        alias: {json.dumps(alias)}")
        lines.append(f"        display-name: {json.dumps(model['display'] + ' (Kilo free)')}")
        if model["context"]:
            lines.append(f"        max-context-length: {model['context']}")
        if model["max_out"]:
            lines.append(f"        max-completion-tokens: {model['max_out']}")
        lines.append(f"        input-modalities: {json.dumps(model['input'])}")
        lines.append(f"        output-modalities: {json.dumps(model['output'])}")
        if "reasoning" in model["params"] or "include_reasoning" in model["params"]:
            lines.append("        thinking:")
            lines.append('          levels: ["low", "medium", "high"]')
    return "\n".join(lines)


def plugin_models(models: list[dict], provider: str) -> str:
    out = []
    for model in models:
        alias = ALIASES.get(model["id"], model["id"].replace("/", "-").replace(":", "-"))
        out.append(f"        - id: {provider}/{alias}")
        out.append(f"          upstream: {json.dumps(model['id'])}")
        out.append(f"          display_name: {json.dumps(model['display'] + ' (Kilo free, rotating)')}")
        out.append(f"          context_length: {model['context']}")
        out.append(f"          max_completion_tokens: {model['max_out']}")
        out.append(f"          input_modalities: {json.dumps(model['input'])}")
        out.append(f"          output_modalities: {json.dumps(model['output'])}")
        if "reasoning" in model["params"]:
            out.append('          thinking_levels: ["none", "low", "high"]')
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--format", choices=["snippet", "plugin", "json"], default="snippet")
    parser.add_argument("--provider", default="kilocode")
    parser.add_argument("--prefix", default="kilocode")
    parser.add_argument("--file", default=None, help="use a cached models JSON instead of fetching")
    args = parser.parse_args(argv)
    if args.file:
        with open(args.file) as handle:
            payload = json.load(handle)
        models = []
        for model in payload:
            arch = model.get("architecture") or {}
            top = model.get("top_provider") or {}
            models.append(
                {
                    "id": model["id"],
                    "display": model.get("name") or model["id"],
                    "context": model.get("context_length") or top.get("context_length") or 0,
                    "max_out": top.get("max_completion_tokens") or 0,
                    "input": arch.get("input_modalities") or ["text"],
                    "output": arch.get("output_modalities") or ["text"],
                    "params": model.get("supported_parameters") or [],
                    "description": (model.get("description") or "").split("\n")[0][:200],
                }
            )
        models.sort(key=lambda m: (-m["context"], m["id"]))
    else:
        models = fetch_free_models()
    if args.format == "json":
        print(json.dumps(models, indent=1))
    elif args.format == "plugin":
        print(plugin_models(models, args.provider))
    else:
        print(snippet(models, args.provider, args.prefix))
    return 0


if __name__ == "__main__":
    sys.exit(main())
