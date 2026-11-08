"""MQTT and limited REST data coordinator for BMW CarData."""

from __future__ import annotations

import logging
import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.helpers.event import async_track_time_interval

from .api import (
    BmwCarDataApiError,
    BmwCarDataOAuthError,
    BmwCarDataRateLimitError,
    BmwCarDataRestApi,
    BmwCarDataRestAuthError,
)
from .const import (
    CONF_BASIC_DATA_BY_VIN,
    CONF_MAPPINGS,
    CONF_REST_AVAILABLE_BY_VIN,
    CONF_REST_DATA_BY_VIN,
    CONF_REST_REQUEST_STATE,
    CONF_SCOPE,
    CONF_STREAM_TOPIC,
    CONF_TELEMATIC_DATA_BY_VIN,
    CONF_USE_REST,
    DEFAULT_STREAM_TOPIC,
    REST_DAILY_CALL_LIMIT,
    REST_SCOPE,
    REST_SENSOR_KEYS,
    REST_UPDATE_HOURS,
)
from .stream_manager import BmwCarDataStreamManager
from .token_manager import BmwCarDataTokenManager

_RestResult = TypeVar("_RestResult")


class BmwCarDataCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Keep MQTT updates independent from quota-limited REST snapshots."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        entry: ConfigEntry,
        stream_manager: BmwCarDataStreamManager,
        rest_api: BmwCarDataRestApi,
        token_manager: BmwCarDataTokenManager,
    ) -> None:
        """Initialize the push coordinator and separate REST polling state."""
        super().__init__(
            hass,
            logger=logging.getLogger(__name__),
            name="BMW CarData",
            update_interval=None,
        )
        self._entry = entry
        self._stream_manager = stream_manager
        self._rest_api = rest_api
        self._token_manager = token_manager
        self._rest_lock = asyncio.Lock()
        self._rest_authorization_failed = False
        self._rest_data_by_vin, self._rest_available_by_vin = self._restore_rest_snapshot()
        self._telematic_cache_by_vin: dict[str, dict[str, dict[str, Any]]] = {}

    def _restore_rest_snapshot(
        self,
    ) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, bool]]:
        """Restore saved readings without treating failed requests as successful."""
        saved = self._entry.data.get(CONF_REST_DATA_BY_VIN, {})
        flags = self._entry.data.get(CONF_REST_AVAILABLE_BY_VIN, {})
        restored: dict[str, dict[str, dict[str, Any]]] = {}
        available: dict[str, bool] = {}
        if not isinstance(saved, dict) or not isinstance(flags, dict):
            self.logger.error("Invalid saved BMW REST snapshot or availability state")
            return restored, available
        for vin, values in saved.items():
            if (
                not isinstance(vin, str) or not vin or not isinstance(values, dict)
                or any(
                    not isinstance(key, str) or not isinstance(value, dict)
                    for key, value in values.items()
                )
            ):
                self.logger.error("Discarding invalid saved BMW REST vehicle data")
                continue
            restored[vin] = {
                key: dict(values[key]) for key in REST_SENSOR_KEYS if key in values
            }
            flag = flags.get(vin, False)
            if not isinstance(flag, bool):
                self.logger.error("Invalid saved BMW REST vehicle availability")
                flag = False
            available[vin] = flag
        return restored, available

    @property
    def use_rest(self) -> bool:
        """Return whether REST sensors are enabled."""
        return bool(self._entry.options.get(CONF_USE_REST, self._entry.data.get(CONF_USE_REST, False)))

    @property
    def use_streaming(self) -> bool:
        """Return whether MQTT configuration and authorization are complete."""
        return self._stream_manager.enabled

    async def async_initialize(self) -> None:
        """Publish saved REST readings and start the MQTT listener."""
        self.async_set_updated_data(self._build_payload())
        await self._stream_manager.async_start()
        if self.use_rest:
            scope = self._entry.data.get(CONF_SCOPE, "")
            if REST_SCOPE not in scope.split():
                self._rest_available_by_vin = {
                    vin: False for vin in self._rest_data_by_vin
                }
                self._publish_rest_snapshot()
                self.logger.warning(
                    "REST sensors require cardata:api:read; starting reauthorization"
                )
                self._entry.async_start_reauth(self.hass)
            else:
                await self.async_refresh_rest()
            self._entry.async_on_unload(
                async_track_time_interval(
                    self.hass, self.async_refresh_rest, timedelta(hours=REST_UPDATE_HOURS)
                )
            )

    async def async_apply_stream_snapshot(self) -> None:
        """Apply the latest incoming MQTT data and update entities."""
        known_vins = {mapping["vin"] for mapping in self._build_stream_mappings()}
        telematic_snapshot = await self._stream_manager.async_get_telematic_snapshot()
        if not telematic_snapshot:
            return

        self._telematic_cache_by_vin = self._merge_telematic_by_vin(
            self._telematic_cache_by_vin,
            telematic_snapshot,
        )
        self.logger.debug(
            "Applying BMW MQTT snapshot with key counts per VIN: %s",
            {
                vin: len(values)
                for vin, values in self._telematic_cache_by_vin.items()
            },
        )
        self.async_set_updated_data(self._build_payload())
        if self.use_rest and any(
            mapping["vin"] not in known_vins for mapping in self._build_stream_mappings()
        ):
            self.hass.async_create_task(self.async_refresh_rest())

    async def async_refresh_rest(self, _now: datetime | None = None) -> None:
        """Poll known VINs at most every two hours, including across reloads."""
        if (
            not self.use_rest
            or self._rest_authorization_failed
            or REST_SCOPE not in self._entry.data.get(CONF_SCOPE, "").split()
        ):
            return
        async with self._rest_lock:
            if self._rest_authorization_failed:
                return
            now = datetime.now(UTC)
            stored = self._entry.data.get(CONF_REST_REQUEST_STATE, {})
            state = {
                **stored,
                "last_attempt_by_vin": dict(stored.get("last_attempt_by_vin", {})),
            }
            if state.get("date") != now.date().isoformat():
                state.update(date=now.date().isoformat(), count=0, blocked=False)
            if state.get("blocked"):
                return

            due_vins = []
            for mapping in self._build_stream_mappings():
                vin = mapping["vin"]
                previous = state["last_attempt_by_vin"].get(vin)
                if previous:
                    try:
                        last_attempt = datetime.fromisoformat(previous)
                        if now - last_attempt < timedelta(hours=REST_UPDATE_HOURS):
                            continue
                    except (ValueError, TypeError):
                        self.logger.error("Invalid persisted BMW REST polling timestamp")
                        state["last_attempt_by_vin"][vin] = now.isoformat()
                        self._save_rest_state(state)
                        continue
                due_vins.append(vin)
            if not due_vins:
                return

            try:
                token = await self._token_manager.async_get_access_token()
                container_id = await self._async_get_rest_container(token, state)
            except (BmwCarDataApiError, BmwCarDataOAuthError, ValueError) as err:
                self._rest_available_by_vin = {
                    mapping["vin"]: False for mapping in self._build_stream_mappings()
                }
                if not self._handle_rest_error(err, state):
                    self.logger.warning("BMW REST container setup unavailable: %s", err)
                self._publish_rest_snapshot()
                return

            for vin in due_vins:
                try:
                    state["last_attempt_by_vin"][vin] = datetime.now(UTC).isoformat()
                    telematic = await self._async_rest_request(
                        state,
                        lambda: self._rest_api.async_get_telematic_data(
                            access_token=token, vin=vin, container_id=container_id
                        ),
                    )
                except (BmwCarDataApiError, BmwCarDataOAuthError, ValueError) as err:
                    self._rest_available_by_vin[vin] = False
                    if self._handle_rest_error(err, state):
                        break
                    self.logger.warning("BMW REST snapshot unavailable: %s", err)
                else:
                    missing = [key for key in REST_SENSOR_KEYS if key not in telematic]
                    if missing:
                        self.logger.warning("BMW REST snapshot is missing fields: %s", missing)
                    self._rest_data_by_vin[vin] = {
                        key: telematic[key] for key in REST_SENSOR_KEYS if key in telematic
                    }
                    self._rest_available_by_vin[vin] = True
            self._publish_rest_snapshot()

    def _publish_rest_snapshot(self) -> None:
        """Persist readings and failure status together before notifying entities."""
        self.hass.config_entries.async_update_entry(
            self._entry,
            data={
                **self._entry.data,
                CONF_REST_DATA_BY_VIN: {
                    vin: {key: dict(value) for key, value in values.items()}
                    for vin, values in self._rest_data_by_vin.items()
                },
                CONF_REST_AVAILABLE_BY_VIN: dict(self._rest_available_by_vin),
            },
        )
        self.async_set_updated_data(self._build_payload())

    async def _async_get_rest_container(
        self, token: str, state: dict[str, Any]
    ) -> str:
        """Reuse the newest compatible active container, checking once per UTC day."""
        now = datetime.now(UTC)
        cached = state.get("container_id")
        if (
            isinstance(cached, str) and cached
            and state.get("container_checked_date") == now.date().isoformat()
        ):
            return cached
        if state.get("container_limit_date") == now.date().isoformat():
            raise BmwCarDataApiError(
                "No compatible container and BMW's 10-container limit is reached; "
                "remove a container manually, then wait for the next UTC day's check"
            )
        previous = state.get("container_discovery_attempt")
        if previous:
            try:
                if now - datetime.fromisoformat(previous) < timedelta(hours=REST_UPDATE_HOURS):
                    raise BmwCarDataApiError("Container discovery will retry after two hours")
            except (TypeError, ValueError) as err:
                self.logger.error("Invalid persisted BMW container discovery timestamp")
                state["container_discovery_attempt"] = now.isoformat()
                self._save_rest_state(state)
                raise BmwCarDataApiError("Invalid container discovery polling state") from err

        state["container_discovery_attempt"] = now.isoformat()
        self._save_rest_state(state)
        containers = await self._async_rest_request(
            state, lambda: self._rest_api.async_list_containers(access_token=token)
        )
        for container in sorted(
            containers,
            key=lambda item: datetime.fromisoformat(item["created"].replace("Z", "+00:00")),
            reverse=True,
        ):
            descriptors = await self._async_rest_request(
                state,
                lambda: self._rest_api.async_get_container_descriptors(
                    access_token=token, container_id=container["containerId"]
                ),
            )
            if set(REST_SENSOR_KEYS).issubset(descriptors):
                container_id = container["containerId"]
                break
        else:
            if len(containers) >= 10:
                state["container_limit_date"] = now.date().isoformat()
                self._save_rest_state(state)
                raise BmwCarDataApiError(
                    "No compatible container and BMW's 10-container limit is reached; "
                    "existing containers will not be deleted"
                )
            container_id = await self._async_rest_request(
                state, lambda: self._rest_api.async_create_container(access_token=token)
            )
            self.logger.info("Created a BMW CarData container for the three REST sensor fields")

        state.update(
            container_id=container_id,
            container_checked_date=datetime.now(UTC).date().isoformat(),
        )
        self._save_rest_state(state)
        return container_id

    async def _async_rest_request(
        self, state: dict[str, Any], request: Callable[[], Awaitable[_RestResult]]
    ) -> _RestResult:
        """Count discovery, creation and data requests against the same daily quota."""
        today = datetime.now(UTC).date().isoformat()
        if state.get("date") != today:
            state.update(date=today, count=0, blocked=False)
        if state.get("blocked") or state["count"] >= REST_DAILY_CALL_LIMIT:
            raise BmwCarDataRateLimitError("Local BMW REST daily call limit reached")
        state["count"] += 1
        self._save_rest_state(state)
        return await request()

    def _handle_rest_error(
        self, err: Exception, state: dict[str, Any]
    ) -> bool:
        """Pause only REST for quota or authorization failures."""
        if isinstance(err, BmwCarDataRateLimitError):
            state["blocked"] = True
            self._save_rest_state(state)
            self.logger.warning("%s; REST polling paused until next UTC day", err)
        elif isinstance(err, (BmwCarDataRestAuthError, BmwCarDataOAuthError)):
            self._rest_authorization_failed = True
            self.logger.error("BMW REST authorization failed: %s", err)
            self._entry.async_start_reauth(self.hass)
        else:
            return False
        self._rest_available_by_vin = {
            mapping["vin"]: False for mapping in self._build_stream_mappings()
        }
        return True

    def _save_rest_state(self, state: dict[str, Any]) -> None:
        """Persist attempts before requests so reloads cannot consume extra quota."""
        self.hass.config_entries.async_update_entry(
            self._entry,
            data={**self._entry.data, CONF_REST_REQUEST_STATE: {
                **state, "last_attempt_by_vin": dict(state["last_attempt_by_vin"])
            }},
        )

    def _build_payload(self) -> dict[str, Any]:
        """Keep REST values separate so other entities still use MQTT."""
        return {
            CONF_MAPPINGS: self._build_stream_mappings(),
            CONF_BASIC_DATA_BY_VIN: {},
            CONF_TELEMATIC_DATA_BY_VIN: dict(self._telematic_cache_by_vin),
            CONF_REST_DATA_BY_VIN: dict(self._rest_data_by_vin),
            CONF_REST_AVAILABLE_BY_VIN: dict(self._rest_available_by_vin),
        }

    def _build_stream_mappings(self) -> list[dict[str, str]]:
        """Build vehicle mappings from the configured topic and received VINs."""
        vins = set(self._telematic_cache_by_vin)
        if self.use_rest:
            vins.update(self._rest_data_by_vin)
        stream_topic = self._entry.options.get(
            CONF_STREAM_TOPIC,
            self._entry.data.get(CONF_STREAM_TOPIC, DEFAULT_STREAM_TOPIC),
        )
        if isinstance(stream_topic, str):
            for segment in stream_topic.split("/"):
                candidate = segment.strip().upper()
                if len(candidate) == 17 and candidate.isalnum():
                    vins.add(candidate)
        return [
            {"vin": vin, "mappingType": "PRIMARY"}
            for vin in sorted(vins)
            if isinstance(vin, str) and vin
        ]

    @staticmethod
    def _merge_telematic_by_vin(
        base: dict[str, dict[str, dict[str, Any]]],
        incoming: dict[str, dict[str, dict[str, Any]]],
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """Merge incoming telematic entries by VIN."""
        merged = {vin: dict(values) for vin, values in base.items()}
        for vin, telematic in incoming.items():
            if not isinstance(vin, str) or not isinstance(telematic, dict):
                continue
            values = merged.setdefault(vin, {})
            for key, entry in telematic.items():
                if isinstance(key, str) and isinstance(entry, dict):
                    values[key] = entry
        return merged
