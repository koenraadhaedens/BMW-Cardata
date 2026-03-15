#!/usr/bin/env python3
"""BMW CarData MQTT topic probe.

This script helps verify if messages are arriving on a BMW streaming topic.
It mirrors the integration logic for topic normalization and auth fallback.

Usage examples:
  python test_mqtt_topic.py --username <GCID> --token <ID_OR_ACCESS_TOKEN> --topic <topic>
  python test_mqtt_topic.py --username <GCID> --id-token <ID_TOKEN> --access-token <ACCESS_TOKEN> --topic <topic>

Notes:
- Streaming requires scope: cardata:streaming:read in the token.
- Host defaults to customer.streaming-cardata.bmwgroup.com
- Port defaults to 9000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import ssl
import sys
from typing import Any

DEFAULT_STREAM_HOST = "customer.streaming-cardata.bmwgroup.com"
DEFAULT_STREAM_PORT = 9000
STREAMING_SCOPE = "cardata:streaming:read"


def build_subscription_topic(username: str, raw_topic: str) -> str:
    topic = raw_topic.strip()
    if not topic:
        return ""
    if topic.startswith(f"{username}/"):
        return topic
    if topic in {"+", "#"}:
        return f"{username}/{topic}"
    if "/" in topic:
        return f"{username}/{topic.split('/', 1)[1]}"
    return f"{username}/{topic}"


def guess_scope_present(token_value: str) -> bool:
    parts = token_value.split(".")
    if len(parts) < 2:
        return False
    payload_b64 = parts[1]
    padding = "=" * (-len(payload_b64) % 4)
    try:
        import base64

        payload = base64.urlsafe_b64decode(payload_b64 + padding).decode("utf-8", errors="ignore")
        data = json.loads(payload)
    except Exception:
        return False

    scope_raw = data.get("scope")
    if isinstance(scope_raw, str):
        return STREAMING_SCOPE in scope_raw

    scp_raw = data.get("scp")
    if isinstance(scp_raw, str):
        return STREAMING_SCOPE in scp_raw
    return False


async def listen_once(
    *,
    host: str,
    port: int,
    username: str,
    password: str,
    topic: str,
    timeout_seconds: int,
) -> int:
    try:
        from aiomqtt import Client, MqttError
    except Exception as err:
        raise RuntimeError("aiomqtt is not installed. Install with: pip install aiomqtt") from err

    tls_context = ssl.create_default_context()
    message_count = 0
    end_at = asyncio.get_running_loop().time() + timeout_seconds

    try:
        async with Client(
            hostname=host,
            port=port,
            username=username,
            password=password,
            tls_context=tls_context,
        ) as client:
            await client.subscribe(topic)
            print(f"[OK] Connected and subscribed to: {topic}")
            print(f"[INFO] Waiting up to {timeout_seconds}s for messages...")

            async for message in client.messages:
                payload = message.payload
                try:
                    text = payload.decode("utf-8")
                except Exception:
                    text = str(payload)

                message_count += 1
                print(f"\n--- message #{message_count} ---")
                print(f"topic: {message.topic.value}")
                print(f"bytes: {len(payload)}")

                try:
                    parsed = json.loads(text)
                    print("json:")
                    print(json.dumps(parsed, indent=2, ensure_ascii=False)[:5000])
                except Exception:
                    print("payload:")
                    print(text[:5000])

                if asyncio.get_running_loop().time() >= end_at:
                    break
    except MqttError as err:
        raise RuntimeError(f"MQTT error: {err}") from err

    return message_count


async def run(args: argparse.Namespace) -> int:
    if not args.username.strip():
        print("[ERROR] --username is required")
        return 2

    raw_topic = args.topic.strip()
    if not raw_topic:
        print("[ERROR] --topic is required")
        return 2

    topic = build_subscription_topic(args.username.strip(), raw_topic)
    if not topic:
        print("[ERROR] Could not build subscription topic")
        return 2

    token_candidates: list[tuple[str, str]] = []
    if args.token:
        token_candidates.append(("token", args.token.strip()))
    if args.id_token:
        token_candidates.append(("id_token", args.id_token.strip()))
    if args.access_token:
        token_candidates.append(("access_token", args.access_token.strip()))

    deduped: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, value in token_candidates:
        if not value or value in seen:
            continue
        seen.add(value)
        deduped.append((name, value))

    if not deduped:
        print("[ERROR] Provide --token or --id-token/--access-token")
        return 2

    print(f"[INFO] Host: {args.host}:{args.port}")
    print(f"[INFO] Username (GCID/client_id): {args.username}")
    print(f"[INFO] Raw topic: {raw_topic}")
    print(f"[INFO] Final topic: {topic}")

    for token_name, token_value in deduped:
        has_scope = guess_scope_present(token_value)
        print(f"[INFO] Trying auth with {token_name}; token_has_streaming_scope={has_scope}")
        try:
            count = await listen_once(
                host=args.host,
                port=args.port,
                username=args.username,
                password=token_value,
                topic=topic,
                timeout_seconds=args.timeout,
            )
            print(f"\n[RESULT] {token_name}: received {count} message(s)")
            if count == 0:
                print("[HINT] Connection worked but no data arrived in the interval. Try driving/waking the car and run again.")
            return 0
        except Exception as err:
            print(f"[WARN] {token_name} failed: {err}")
            continue

    print("\n[RESULT] All token candidates failed.")
    print("[HINT] Most common causes: missing cardata:streaming:read scope, wrong topic, wrong username, expired token.")
    return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe BMW MQTT topic and print incoming messages")
    parser.add_argument("--host", default=DEFAULT_STREAM_HOST, help="MQTT host")
    parser.add_argument("--port", type=int, default=DEFAULT_STREAM_PORT, help="MQTT port")
    parser.add_argument("--username", required=True, help="GCID/client id used as MQTT username")
    parser.add_argument("--topic", required=True, help="Configured streaming topic")

    parser.add_argument("--token", help="Single token to test as MQTT password")
    parser.add_argument("--id-token", dest="id_token", help="ID token candidate")
    parser.add_argument("--access-token", dest="access_token", help="Access token candidate")
    parser.add_argument("--timeout", type=int, default=90, help="How long to wait for messages")
    return parser.parse_args()


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        raise SystemExit(asyncio.run(run(parse_args())))
    except KeyboardInterrupt:
        print("\nInterrupted")
        raise SystemExit(130)
