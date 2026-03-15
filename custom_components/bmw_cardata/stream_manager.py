"""BMW CarData MQTT stream manager."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import json
import logging
import ssl
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_CLIENT_ID
from homeassistant.core import HomeAssistant

from .const import (
    CONF_SCOPE,
    CONF_GCID,
    CONF_ID_TOKEN,
    CONF_STREAM_HOST,
    CONF_STREAM_PORT,
    CONF_STREAM_TOPIC,
    DEFAULT_STREAM_HOST,
    DEFAULT_STREAM_PORT,
    STREAMING_SCOPE,
)
from .token_manager import BmwCarDataTokenManager


class BmwCarDataStreamManager:
    """Manage outbound MQTT stream connection and telematic cache."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        entry: ConfigEntry,
        token_manager: BmwCarDataTokenManager,
        on_updates: Callable[[], Any] | None = None,
    ) -> None:
        """Initialize stream manager."""
        self._hass = hass
        self._entry = entry
        self._token_manager = token_manager
        self._logger = logging.getLogger(__name__)
        self._telematic_by_vin: dict[str, dict[str, dict[str, Any]]] = {}
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._on_updates = on_updates

    def _config_value(self, key: str) -> Any:
        """Get configuration value from options with fallback to entry data."""
        if key in self._entry.options:
            return self._entry.options.get(key)
        if key in self._entry.data:
            return self._entry.data.get(key)
        if key == CONF_STREAM_HOST:
            return DEFAULT_STREAM_HOST
        if key == CONF_STREAM_PORT:
            return DEFAULT_STREAM_PORT
        return None

    @property
    def enabled(self) -> bool:
        """Return whether stream configuration is complete."""
        host = self._config_value(CONF_STREAM_HOST)
        topic = self._config_value(CONF_STREAM_TOPIC)
        gcid = self._entry.data.get(CONF_GCID)
        scope_raw = self._entry.data.get(CONF_SCOPE)
        has_scope = isinstance(scope_raw, str) and STREAMING_SCOPE in scope_raw
        return (
            isinstance(host, str)
            and bool(host)
            and isinstance(topic, str)
            and bool(topic)
            and isinstance(gcid, str)
            and bool(gcid)
            and has_scope
        )

    async def async_start(self) -> None:
        """Start MQTT listener task."""
        if not self.enabled:
            return
        if self._task and not self._task.done():
            return

        self._stop_event.clear()
        self._task = self._hass.async_create_task(self._async_run())

    async def async_stop(self) -> None:
        """Stop MQTT listener task."""
        self._stop_event.set()
        task = self._task
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def async_get_telematic_snapshot(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Get a copy of latest streamed telematic entries by VIN."""
        async with self._lock:
            return {
                vin: dict(values)
                for vin, values in self._telematic_by_vin.items()
            }

    async def _async_run(self) -> None:
        """Run MQTT connection loop with reconnect backoff."""
        backoff_seconds = 5

        while not self._stop_event.is_set():
            try:
                await self._async_connect_once()
                backoff_seconds = 5
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self._logger.warning("BMW stream disconnected: %s", err)
                err_text = str(err).lower()
                if "streaming scope missing" in err_text or "authorization failed" in err_text:
                    backoff_seconds = 300
                await asyncio.sleep(backoff_seconds)
                if backoff_seconds < 300:
                    backoff_seconds = min(backoff_seconds * 2, 60)

    async def _async_connect_once(self) -> None:
        """Connect once and process stream messages until disconnect."""
        try:
            from aiomqtt import Client, MqttError
        except Exception as err:
            raise RuntimeError("aiomqtt is not installed") from err

        host = self._config_value(CONF_STREAM_HOST)
        port = int(self._config_value(CONF_STREAM_PORT) or DEFAULT_STREAM_PORT)
        username = self._entry.data.get(CONF_GCID) or self._entry.data.get(CONF_CLIENT_ID)
        if not isinstance(username, str) or not username:
            raise RuntimeError("Streaming username missing")

        scope_raw = self._entry.data.get(CONF_SCOPE)
        if not isinstance(scope_raw, str) or STREAMING_SCOPE not in scope_raw:
            raise RuntimeError(
                "Streaming scope missing in token. Re-add integration with streaming enabled to request cardata:streaming:read."
            )

        topic = self._build_subscription_topic(username, self._config_value(CONF_STREAM_TOPIC))
        if not topic:
            raise RuntimeError("Streaming topic missing")

        tls_context = await self._hass.async_add_executor_job(ssl.create_default_context)

        auth_candidates: list[tuple[str, str]] = []
        try:
            id_token = await self._token_manager.async_get_id_token()
            if isinstance(id_token, str) and id_token:
                auth_candidates.append(("id_token", id_token))
        except Exception as err:
            self._logger.debug("ID token unavailable for MQTT auth: %s", err)

        try:
            access_token = await self._token_manager.async_get_access_token()
            if isinstance(access_token, str) and access_token:
                auth_candidates.append(("access_token", access_token))
        except Exception as err:
            self._logger.debug("Access token unavailable for MQTT auth: %s", err)

        deduped_candidates: list[tuple[str, str]] = []
        seen_values: set[str] = set()
        for token_name, token_value in auth_candidates:
            if token_value in seen_values:
                continue
            seen_values.add(token_value)
            deduped_candidates.append((token_name, token_value))

        if not deduped_candidates:
            raise RuntimeError("No valid MQTT auth token available")

        last_error: Exception | None = None
        for token_name, token_value in deduped_candidates:
            self._logger.debug(
                "BMW stream connecting host=%s port=%s username=%s topic=%s auth=%s",
                host,
                port,
                username,
                topic,
                token_name,
            )
            try:
                async with Client(
                    hostname=host,
                    port=port,
                    username=username,
                    password=token_value,
                    tls_context=tls_context,
                ) as client:
                    await client.subscribe(topic)
                    self._logger.debug(
                        "BMW stream connected to %s:%s topic=%s auth=%s",
                        host,
                        port,
                        topic,
                        token_name,
                    )
                    async for message in client.messages:
                        if self._stop_event.is_set():
                            return
                        await self._async_handle_message(message.topic.value, message.payload)
            except MqttError as err:
                message = str(err)
                message_l = message.lower()
                if (
                    "not authorized" in message_l
                    or "bad user name or password" in message_l
                    or "code:135" in message_l
                    or "code:134" in message_l
                ):
                    self._logger.warning(
                        "BMW stream MQTT auth rejected using %s; trying next token if available",
                        token_name,
                    )
                    last_error = err
                    continue
                raise RuntimeError(f"MQTT error: {err}") from err

        raise RuntimeError(
            "MQTT authorization failed (Not authorized) for all token types. Re-authenticate BMW and verify stream topic/username pairing."
        ) from last_error

    async def _async_handle_message(self, topic: str, payload: bytes) -> None:
        """Handle incoming MQTT payload."""
        self._logger.debug("BMW stream message received topic=%s bytes=%s", topic, len(payload))
        try:
            decoded = payload.decode("utf-8")
            data = json.loads(decoded)
        except Exception:
            self._logger.debug("Ignoring non-JSON stream payload")
            return

        updates_by_vin = self._extract_telematic_updates(topic, data)
        if not updates_by_vin:
            payload_keys = list(data.keys())[:10] if isinstance(data, dict) else []
            self._logger.debug(
                "BMW stream payload parsed but no telematic updates extracted topic=%s keys=%s",
                topic,
                payload_keys,
            )
            return

        updates_summary = {vin: len(entries) for vin, entries in updates_by_vin.items()}
        self._logger.debug(
            "BMW stream updates extracted topic=%s vins=%s",
            topic,
            updates_summary,
        )

        async with self._lock:
            for vin, updates in updates_by_vin.items():
                existing = self._telematic_by_vin.get(vin, {})
                merged = dict(existing)
                merged.update(updates)
                self._telematic_by_vin[vin] = merged

        if self._on_updates is not None:
            try:
                result = self._on_updates()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as err:
                self._logger.debug("Stream update callback failed: %s", err)

    def _extract_telematic_updates(
        self,
        topic: str,
        payload: Any,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """Extract telematic key/value updates grouped by VIN from stream payload."""
        vin_from_topic = self._extract_vin_from_topic(topic)
        updates_by_vin: dict[str, dict[str, dict[str, Any]]] = {}

        if isinstance(payload, dict):
            if isinstance(payload.get("telematicData"), dict):
                vin = self._extract_vin_from_payload(payload) or vin_from_topic
                if vin:
                    telematic_data = {
                        key: value
                        for key, value in payload["telematicData"].items()
                        if isinstance(key, str) and isinstance(value, dict)
                    }
                    updates_by_vin[vin] = telematic_data
                return updates_by_vin

            if isinstance(payload.get("data"), dict):
                vin = self._extract_vin_from_payload(payload) or vin_from_topic
                if vin:
                    payload_timestamp = payload.get("timestamp")
                    data_values: dict[str, dict[str, Any]] = {}
                    for key, value in payload["data"].items():
                        if not isinstance(key, str) or not isinstance(value, dict):
                            continue
                        normalized = dict(value)
                        if (
                            isinstance(payload_timestamp, str)
                            and not isinstance(normalized.get("timestamp"), str)
                        ):
                            normalized["timestamp"] = payload_timestamp
                        data_values[key] = normalized
                    if data_values:
                        updates_by_vin[vin] = data_values
                return updates_by_vin

            if isinstance(payload.get("name"), str):
                vin = self._extract_vin_from_payload(payload) or vin_from_topic
                if vin:
                    updates_by_vin[vin] = {
                        payload["name"]: {
                            "value": payload.get("value"),
                            "unit": payload.get("unit"),
                            "timestamp": payload.get("timestamp"),
                        }
                    }
                return updates_by_vin

            entries = payload.get("entries")
            if isinstance(entries, list):
                vin = self._extract_vin_from_payload(payload) or vin_from_topic
                if vin:
                    entry_values: dict[str, dict[str, Any]] = {}
                    for item in entries:
                        if not isinstance(item, dict):
                            continue
                        name = item.get("name")
                        if not isinstance(name, str):
                            continue
                        entry_values[name] = {
                            "value": item.get("value"),
                            "unit": item.get("unit"),
                            "timestamp": item.get("timestamp"),
                        }
                    if entry_values:
                        updates_by_vin[vin] = entry_values
                return updates_by_vin

        return updates_by_vin

    @staticmethod
    def _extract_vin_from_payload(payload: dict[str, Any]) -> str | None:
        """Extract VIN from known payload properties."""
        candidates = (
            payload.get("vin"),
            payload.get("vehicleVin"),
            payload.get("vehicleIdentificationNumber"),
            payload.get("vehicleId"),
        )
        for candidate in candidates:
            if isinstance(candidate, str) and len(candidate.strip()) == 17:
                return candidate.strip().upper()
        return None

    @staticmethod
    def _extract_vin_from_topic(topic: str) -> str | None:
        """Try deriving VIN from topic path segments."""
        for segment in topic.split("/"):
            stripped = segment.strip()
            if len(stripped) == 17 and stripped.isalnum():
                return stripped.upper()
        return None

    @staticmethod
    def _build_subscription_topic(username: str, raw_topic: Any) -> str:
        """Build broker subscription topic from user input.

        Accepts raw VIN/topic and auto-prefixes with username when needed.
        """
        if not isinstance(raw_topic, str):
            return ""
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
