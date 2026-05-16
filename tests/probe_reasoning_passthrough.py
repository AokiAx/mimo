#!/usr/bin/env python3
"""
Dual-arm probe: does NewAPI preserve `reasoning_content`, `thinking` blocks, and
`tool_call_id` round-trip?

Runs the same 2-turn tool-call dialogue against two endpoints:

    --direct-url   our gateway directly (control arm)
    --newapi-url   through NewAPI (test arm)

For each arm, dumps:

  * Turn 1 raw assistant message (looking for `reasoning_content`)
  * Turn 2 request body sent (looking for whether reasoning_content survives
    the round-trip when the script *itself* puts it back into messages)
  * Turn 2 raw assistant message
  * All tool_call IDs (turn-1 ids and turn-2 ids)

Then prints a comparison table answering the three open questions:

  A) Does NewAPI strip `reasoning_content` from responses it relays to clients?
  B) Does NewAPI strip `reasoning_content` from requests it relays upstream?
  C) Does NewAPI rewrite `tool_call_id` values?

Usage
-----

    python tests/probe_reasoning_passthrough.py \
        --direct-url   https://your-gateway.example/v1 \
        --direct-key   sk-direct-key \
        --newapi-url   https://newapi.example/v1 \
        --newapi-key   sk-newapi-key \
        --model        mimo-v2.5-pro

If you only have one arm available, pass only that arm — the script will still
dump its captures so you can eyeball them.

The B test relies on a side-channel: turn 2 must hit our gateway either
directly or via NewAPI, and our gateway must log the raw inbound body. Set
`MIMO_PROBE_DUMP=/tmp/inbound.jsonl` on the gateway process before running this
probe so the gateway-side capture lands somewhere known (see
gateway/probe_dump.py if/when added).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any


# ---------- HTTP ----------

def _post_json(url: str, key: str, body: dict[str, Any]) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  HTTP {e.code} from {url}", file=sys.stderr)
        print(f"  body: {body[:500]}", file=sys.stderr)
        raise


# ---------- Fixed conversation ----------

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_current_weather",
            "description": "Get the current weather for a given city",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {"type": "string"},
                    "unit": {"type": "string", "enum": ["celsius", "fahrenheit"]},
                },
                "required": ["location"],
            },
        },
    },
]

USER_TURN_1 = "How is the weather in Beijing today?"
USER_TURN_2 = "How about Shanghai? Compare with Beijing please."


def _fake_tool_result(name: str, args: dict[str, Any]) -> str:
    loc = args.get("location", "")
    return {
        "Beijing": "Sunny 25°C",
        "Shanghai": "Cloudy 22°C",
    }.get(loc, f"Weather for {loc} unavailable")


# ---------- One arm ----------

def run_arm(arm_name: str, base_url: str, key: str, model: str) -> dict[str, Any]:
    """Drive a 2-turn tool-call dialogue. Capture every wire payload."""
    chat_url = base_url.rstrip("/") + "/chat/completions"
    capture: dict[str, Any] = {"arm": arm_name, "base_url": base_url, "turns": []}

    messages: list[dict[str, Any]] = [{"role": "user", "content": USER_TURN_1}]

    for turn_idx, user_msg in enumerate([USER_TURN_1, USER_TURN_2], start=1):
        if turn_idx == 2:
            messages.append({"role": "user", "content": user_msg})

        # Iterate inner request-response loop until model stops tool-calling.
        sub = 0
        while True:
            sub += 1
            body = {
                "model": model,
                "messages": messages,
                "tools": TOOLS,
                "stream": False,
                # Enable MiMo thinking mode (per docs, this is the trigger).
                "thinking": {"type": "enabled"},
            }
            print(f"[{arm_name}] turn {turn_idx}.{sub} → POST {chat_url}")
            t0 = time.time()
            resp = _post_json(chat_url, key, body)
            dt_ms = int((time.time() - t0) * 1000)

            # Capture
            msg = (resp.get("choices") or [{}])[0].get("message") or {}
            capture["turns"].append({
                "turn": f"{turn_idx}.{sub}",
                "request_body": body,           # what we sent
                "response_message": msg,        # raw assistant message we received
                "response_finish": (resp.get("choices") or [{}])[0].get("finish_reason"),
                "latency_ms": dt_ms,
            })

            # Append assistant message to history verbatim (this is the key
            # protocol contract — must preserve reasoning_content).
            messages.append(msg)

            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                break

            # Execute fake tools and append results.
            for tc in tool_calls:
                fn = tc.get("function") or {}
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}
                result = _fake_tool_result(fn.get("name", ""), args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id", ""),
                    "content": result,
                })

    return capture


# ---------- Analysis ----------

def _collect_tool_ids(turns: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for t in turns:
        for tc in (t["response_message"].get("tool_calls") or []):
            ids.append(tc.get("id", ""))
    return ids


def _has_reasoning_in_response(turns: list[dict[str, Any]]) -> list[bool]:
    return [
        bool((t["response_message"].get("reasoning_content") or "").strip())
        for t in turns
    ]


def _has_reasoning_in_request_history(turns: list[dict[str, Any]]) -> list[bool]:
    """For each request body, scan the assistant messages in `messages` for
    reasoning_content. Skips the first request of each turn (no history yet).
    """
    out: list[bool] = []
    for t in turns:
        msgs = t["request_body"].get("messages") or []
        asst_msgs = [m for m in msgs if m.get("role") == "assistant"]
        if not asst_msgs:
            out.append(True)   # vacuously OK
            continue
        has_any = any(
            bool((m.get("reasoning_content") or "").strip())
            for m in asst_msgs
        )
        out.append(has_any)
    return out


def analyze(captures: list[dict[str, Any]]) -> None:
    print("\n" + "=" * 70)
    print("ANALYSIS")
    print("=" * 70)

    for cap in captures:
        arm = cap["arm"]
        turns = cap["turns"]
        ids = _collect_tool_ids(turns)
        has_resp = _has_reasoning_in_response(turns)
        has_req_hist = _has_reasoning_in_request_history(turns)

        print(f"\n--- arm: {arm} ({cap['base_url']}) ---")
        print(f"  sub-turns           : {len(turns)}")
        print(f"  finish_reasons      : {[t['response_finish'] for t in turns]}")
        print(f"  reasoning in resp   : {has_resp}")
        print(f"  reasoning in req his: {has_req_hist}")
        print(f"  tool_call_ids       : {ids}")

        # A: did we see reasoning_content in any response?
        if any(has_resp):
            print("  [A] reasoning_content present in responses: YES (good)")
        else:
            print("  [A] reasoning_content present in responses: NO")
            print("       → if direct arm: gateway/upstream is dropping it")
            print("       → if newapi arm: NewAPI may be stripping it (or direct arm also missing)")

    # Cross-arm comparison if both present.
    if len(captures) == 2:
        a, b = captures
        ids_a = _collect_tool_ids(a["turns"])
        ids_b = _collect_tool_ids(b["turns"])
        print("\n--- cross-arm comparison ---")
        print(f"  ids[{a['arm']}]: {ids_a}")
        print(f"  ids[{b['arm']}]: {ids_b}")
        if ids_a and ids_b:
            shared_prefix = os.path.commonprefix([ids_a[0], ids_b[0]])
            print(f"  first-id common prefix: {shared_prefix!r}")
            print("  [C] If NewAPI rewrites tool_call_id, the prefixes differ.")
            print("       Identical schema (e.g. both start 'call_') is normal;")
            print("       value identity across arms is NOT expected (different upstream calls).")

    print("\n" + "=" * 70)
    print("WHAT TO READ NEXT")
    print("=" * 70)
    print("""\
A (response strip):
  Compare 'reasoning in resp' between direct arm and newapi arm.
  Direct has TRUE somewhere but NewAPI all FALSE  →  NewAPI strips on response.

B (request strip):
  This script *always* appends the raw assistant message to messages, so
  'reasoning in req his' should be TRUE on every sub-turn after the first.
  If you also configured MIMO_PROBE_DUMP on the gateway, cross-check what
  the gateway *received* — if NewAPI strips during forwarding, gateway-side
  inbound dump will show reasoning_content missing even though this script
  sent it.

C (id rewrite):
  If gateway-side inbound dump shows different ids than what this script
  thinks it sent, NewAPI is rewriting ids. Without gateway-side dump, this
  test cannot prove C — only the response side is visible to the client.

The full per-turn capture (request + response bodies) is saved as JSON to
the path you passed via --output, so you can grep it yourself.""")


# ---------- Entrypoint ----------

def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--direct-url", help="Direct gateway base URL, e.g. https://host/v1")
    p.add_argument("--direct-key", help="Bearer token for direct gateway")
    p.add_argument("--newapi-url", help="NewAPI base URL, e.g. https://newapi/v1")
    p.add_argument("--newapi-key", help="Bearer token for NewAPI")
    p.add_argument("--model", default="mimo-v2.5-pro")
    p.add_argument("--output", default="probe_reasoning_passthrough.json")
    args = p.parse_args()

    if not (args.direct_url or args.newapi_url):
        p.error("At least one of --direct-url / --newapi-url is required")

    captures: list[dict[str, Any]] = []

    if args.direct_url:
        if not args.direct_key:
            p.error("--direct-key required when --direct-url is set")
        try:
            captures.append(run_arm("direct", args.direct_url, args.direct_key, args.model))
        except Exception as e:
            print(f"[direct] failed: {e}", file=sys.stderr)

    if args.newapi_url:
        if not args.newapi_key:
            p.error("--newapi-key required when --newapi-url is set")
        try:
            captures.append(run_arm("newapi", args.newapi_url, args.newapi_key, args.model))
        except Exception as e:
            print(f"[newapi] failed: {e}", file=sys.stderr)

    if not captures:
        print("No successful arms; nothing to analyze.", file=sys.stderr)
        return 1

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(captures, f, ensure_ascii=False, indent=2)
    print(f"\nFull capture saved → {args.output}")

    analyze(captures)
    return 0


if __name__ == "__main__":
    sys.exit(main())
