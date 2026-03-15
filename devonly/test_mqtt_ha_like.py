#!/usr/bin/env python3
"""BMW MQTT probe with HA-like device code flow.

This script reproduces the Home Assistant auth experience:
1) Start BMW device-code auth
2) Open verification URL and authorize
3) Poll token endpoint
4) Connect to MQTT and print incoming messages

Usage:
  python test_mqtt_ha_like.py --client-id <CLIENT_ID> --stream-topic <TOPIC>

Optional:
  --host customer.streaming-cardata.bmwgroup.com
  --port 9000
  --timeout 120
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import secrets
import ssl
import sys
from dataclasses import dataclass
from typing import Any

import aiohttp

OAUTH_BASE_URL = "https://customer.bmwgroup.com"
DEVICE_CODE_PATH = "/gcdm/oauth/device/code"
TOKEN_PATH = "/gcdm/oauth/token"
DEFAULT_SCOPE = "authenticate_user openid cardata:api:read"
STREAMING_SCOPE = "cardata:streaming:read"
DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
DEVICE_CODE_RESPONSE_TYPE = "device_code"
PKCE_CHALLENGE_METHOD = "S256"

DEFAULT_STREAM_HOST = "customer.streaming-cardata.bmwgroup.com"
DEFAULT_STREAM_PORT = 9000


@dataclass(slots=True)
class DeviceCodeResponse:
    device_code: str
    user_code: str
    verification_uri: str
    expires_in: int
    interval: int


@dataclass(slots=True)
class TokenResponse:
    access_token: str
    token_type: str
    expires_in: int
    refresh_token: str | None
    scope: str | None
    gcid: str | None
    id_token: str | None


class OAuthFlowError(Exception):
    def __init__(self, error: str, description: str | None = None) -> None:
        super().__init__(description or error)
        self.error = error
        self.description = description


def generate_pkce_pair() -> tuple[str, str]:
    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    return code_verifier, code_challenge


def build_subscription_topic(username: str, raw_topic: str) -> str:
    topic = raw_topic.strip()
    if topic.startswith(f"{username}/"):
        return topic
    if topic in {"+", "#"}:
        return f"{username}/{topic}"
    if "/" in topic:
        return f"{username}/{topic.split('/', 1)[1]}"
    return f"{username}/{topic}"


def decode_scope(token: str) -> str:
    parts = token.split(".")
    if len(parts) < 2:
        return ""
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        raw = base64.urlsafe_b64decode(payload + padding).decode("utf-8", errors="ignore")
        data = json.loads(raw)
    except Exception:
        return ""

    for key in ("scope", "scp"):
        value = data.get(key)
        if isinstance(value, str):
            return value
    return ""


async def post_form(session: aiohttp.ClientSession, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    async with session.post(
        f"{OAUTH_BASE_URL}{path}",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=payload,
    ) as resp:
        data = await resp.json(content_type=None)
        if resp.status >= 400:
            if isinstance(data, dict) and data.get("error"):
                raise OAuthFlowError(data.get("error", "unknown_error"), data.get("error_description"))
            raise RuntimeError(f"OAuth endpoint returned {resp.status}")
        if not isinstance(data, dict):
            raise RuntimeError("Unexpected non-JSON OAuth response")
        return data


async def request_device_code(
    session: aiohttp.ClientSession,
    *,
    client_id: str,
    scope: str,
    code_challenge: str,
) -> DeviceCodeResponse:
    payload = {
        "client_id": client_id,
        "response_type": DEVICE_CODE_RESPONSE_TYPE,
        "scope": scope,
        "code_challenge": code_challenge,
        "code_challenge_method": PKCE_CHALLENGE_METHOD,
    }
    data = await post_form(session, DEVICE_CODE_PATH, payload)
    return DeviceCodeResponse(
        device_code=data["device_code"],
        user_code=data["user_code"],
        verification_uri=data["verification_uri"],
        expires_in=int(data.get("expires_in", 0)),
        interval=int(data.get("interval", 5)),
    )


async def poll_token(
    session: aiohttp.ClientSession,
    *,
    client_id: str,
    device_code: str,
    code_verifier: str,
    interval: int,
    expires_in: int,
) -> TokenResponse:
    deadline = asyncio.get_running_loop().time() + expires_in
    wait_seconds = max(1, interval)

    while asyncio.get_running_loop().time() < deadline:
        try:
            data = await post_form(
                session,
                TOKEN_PATH,
                {
                    "client_id": client_id,
                    "device_code": device_code,
                    "grant_type": DEVICE_CODE_GRANT_TYPE,
                    "code_verifier": code_verifier,
                },
            )
            return TokenResponse(
                access_token=data["access_token"],
                token_type=data["token_type"],
                expires_in=int(data.get("expires_in", 0)),
                refresh_token=data.get("refresh_token"),
                scope=data.get("scope"),
                gcid=data.get("gcid"),
                id_token=data.get("id_token"),
            )
        except OAuthFlowError as err:
            if err.error == "authorization_pending":
                print("[AUTH] Waiting for BMW authorization...")
                await asyncio.sleep(wait_seconds)
                continue
            if err.error == "slow_down":
                wait_seconds += 5
                print(f"[AUTH] Slow down requested; new poll interval={wait_seconds}s")
                await asyncio.sleep(wait_seconds)
                continue
            raise

    raise TimeoutError("Device-code flow timed out before authorization completed")


async def mqtt_probe(
    *,
    host: str,
    port: int,
    username: str,
    topics: list[str],
    id_token: str | None,
    access_token: str,
    timeout: int,
) -> int:
    try:
        from aiomqtt import Client, MqttError
    except Exception as err:
        raise RuntimeError("aiomqtt is not installed. Run: pip install aiomqtt") from err

    candidates: list[tuple[str, str]] = []
    if isinstance(id_token, str) and id_token:
        candidates.append(("id_token", id_token))
    candidates.append(("access_token", access_token))

    seen: set[str] = set()
    deduped: list[tuple[str, str]] = []
    for label, token in candidates:
        if token in seen:
            continue
        seen.add(token)
        deduped.append((label, token))

    tls_context = ssl.create_default_context()

    for topic in topics:
        print(f"[MQTT] Topic probe: {topic}")
        for label, token in deduped:
            try:
                print(f"[MQTT] Trying {label}...")
                msg_count = 0
                async with Client(
                    hostname=host,
                    port=port,
                    username=username,
                    password=token,
                    tls_context=tls_context,
                ) as client:
                    await client.subscribe(topic)
                    print(f"[MQTT] Connected and subscribed: {topic}")
                    print(f"[MQTT] Waiting up to {timeout}s for messages...")

                    end_at = asyncio.get_running_loop().time() + timeout
                    async for message in client.messages:
                        msg_count += 1
                        try:
                            text = message.payload.decode("utf-8")
                        except Exception:
                            text = str(message.payload)

                        print(f"\n--- message #{msg_count} ---")
                        print(f"topic: {message.topic.value}")
                        print(f"bytes: {len(message.payload)}")
                        try:
                            obj = json.loads(text)
                            print(json.dumps(obj, indent=2, ensure_ascii=False)[:5000])
                        except Exception:
                            print(text[:5000])

                        if asyncio.get_running_loop().time() >= end_at:
                            break

                print(f"[MQTT] {label} success on topic {topic}. received={msg_count}")
                return 0
            except MqttError as err:
                print(f"[MQTT] {label} failed on topic {topic}: {err}")
                continue

    return 1


async def run(args: argparse.Namespace) -> int:
    if not args.client_id.strip():
        print("[ERROR] --client-id is required")
        return 2
    if not args.stream_topic.strip():
        print("[ERROR] --stream-topic is required")
        return 2

    requested_scope = DEFAULT_SCOPE
    if STREAMING_SCOPE not in requested_scope:
        requested_scope = f"{requested_scope} {STREAMING_SCOPE}"

    code_verifier, code_challenge = generate_pkce_pair()

    timeout_cfg = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
        device = await request_device_code(
            session,
            client_id=args.client_id.strip(),
            scope=requested_scope,
            code_challenge=code_challenge,
        )

        verification_uri_complete = f"{device.verification_uri}?user_code={device.user_code}"
        print("\n=== BMW AUTHORIZATION REQUIRED ===")
        print(f"Verification URL: {device.verification_uri}")
        print(f"User code       : {device.user_code}")
        print(f"Direct URL      : {verification_uri_complete}")
        print(f"Expires in      : {device.expires_in}s")
        print("Authorize in browser now. Polling for token...")

        token = await poll_token(
            session,
            client_id=args.client_id.strip(),
            device_code=device.device_code,
            code_verifier=code_verifier,
            interval=device.interval,
            expires_in=device.expires_in,
        )

    username = token.gcid or args.client_id.strip()
    topic_primary = build_subscription_topic(username, args.stream_topic.strip())
    topic_wildcard = build_subscription_topic(username, "+")
    topics_to_probe = [topic_primary]
    if args.probe_all_vins and topic_wildcard not in topics_to_probe:
        topics_to_probe.append(topic_wildcard)

    scope = token.scope or decode_scope(token.access_token)
    print("\n=== TOKEN ACQUIRED ===")
    print(f"GCID            : {token.gcid}")
    print(f"MQTT username   : {username}")
    print(f"Token scope     : {scope}")
    print(f"Has stream scope: {STREAMING_SCOPE in scope if isinstance(scope, str) else False}")
    print(f"Final topic     : {topic_primary}")
    if args.probe_all_vins:
        print(f"Wildcard topic  : {topic_wildcard}")
    print("[INFO] Ensure Home Assistant is not using streaming concurrently (one connection per GCID).")

    status = await mqtt_probe(
        host=args.host,
        port=args.port,
        username=username,
        topics=topics_to_probe,
        id_token=token.id_token,
        access_token=token.access_token,
        timeout=args.timeout,
    )

    if status == 0:
        print("\n[RESULT] MQTT probe finished.")
        return 0

    print("\n[RESULT] MQTT connection/auth failed for all token candidates.")
    print("[HINT] Verify stream topic, client id, and that consent includes cardata:streaming:read.")
    return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BMW MQTT probe with HA-like device auth flow")
    parser.add_argument("--client-id", required=True, help="BMW CarData client_id")
    parser.add_argument("--stream-topic", required=True, help="Configured BMW stream topic")
    parser.add_argument(
        "--probe-all-vins",
        action="store_true",
        help="Also probe wildcard subscription username/+ to detect any VIN traffic",
    )
    parser.add_argument("--host", default=DEFAULT_STREAM_HOST, help="MQTT host")
    parser.add_argument("--port", type=int, default=DEFAULT_STREAM_PORT, help="MQTT port")
    parser.add_argument("--timeout", type=int, default=120, help="Seconds to wait for MQTT messages")
    return parser.parse_args()


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        raise SystemExit(asyncio.run(run(parse_args())))
    except KeyboardInterrupt:
        print("\nInterrupted")
        raise SystemExit(130)
