"""MQTT and limited REST data coordinator for BMW CarData."""

from __future__ import annotations

import logging
import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

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
    CONF_REST_CONTAINER_ID,
    CONF_REST_DATA_BY_VIN,
    CONF_REST_REQUEST_STATE,
    CONF_SCOPE,
    CONF_STREAM_TOPIC,
    CONF_TELEMATIC_DATA_BY_VIN,
    REST_DAILY_CALL_LIMIT,
    REST_SCOPE,
    REST_SENSOR_KEYS,
    REST_UPDATE_HOURS,
)
from .stream_manager import BmwCarDataStreamManager
from .token_manager import BmwCarDataTokenManager


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
        self._rest_data_by_vin: dict[str, dict[str, dict[str, Any]]] = {}
        self._rest_available_by_vin: dict[str, bool] = {}
        self._telematic_cache_by_vin: dict[str, dict[str, dict[str, Any]]] = {}

    @property
    def rest_container_id(self) -> str:
        """Return the configured existing REST container."""
        return self._entry.options.get(
            CONF_REST_CONTAINER_ID, self._entry.data.get(CONF_REST_CONTAINER_ID, "")
        )

    @property
    def use_rest(self) -> bool:
        """Return whether REST sensors are enabled."""
        return bool(self.rest_container_id)

    @property
    def use_streaming(self) -> bool:
        """Return whether MQTT configuration and authorization are complete."""
        return self._stream_manager.enabled

    async def async_initialize(self) -> None:
        """Initialize empty state and start the MQTT listener."""
        self.async_set_updated_data(self._build_payload())
        await self._stream_manager.async_start()
        if self.use_rest:
            scope = self._entry.data.get(CONF_SCOPE, "")
            if REST_SCOPE not in scope.split():
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
                if state["count"] >= REST_DAILY_CALL_LIMIT:
                    self.logger.warning("Local BMW REST daily call limit reached")
                    break

                try:
                    token = await self._token_manager.async_get_access_token()
                    state["last_attempt_by_vin"][vin] = now.isoformat()
                    state["count"] += 1
                    self._save_rest_state(state)
                    telematic = await self._rest_api.async_get_telematic_data(
                        access_token=token, vin=vin, container_id=self.rest_container_id
                    )
                except BmwCarDataRateLimitError as err:
                    state["blocked"] = True
                    self._save_rest_state(state)
                    self._rest_available_by_vin = {
                        item["vin"]: False for item in self._build_stream_mappings()
                    }
                    self.logger.warning("%s; REST polling paused until next UTC day", err)
                    break
                except (BmwCarDataRestAuthError, BmwCarDataOAuthError) as err:
                    self._rest_authorization_failed = True
                    self._rest_available_by_vin = {
                        item["vin"]: False for item in self._build_stream_mappings()
                    }
                    self.logger.error("BMW REST authorization failed: %s", err)
                    self._entry.async_start_reauth(self.hass)
                    break
                except (BmwCarDataApiError, ValueError) as err:
                    self._rest_available_by_vin[vin] = False
                    self.logger.warning("BMW REST snapshot unavailable: %s", err)
                else:
                    missing = [key for key in REST_SENSOR_KEYS if key not in telematic]
                    if missing:
                        self.logger.warning("BMW REST snapshot is missing fields: %s", missing)
                    self._rest_data_by_vin[vin] = {
                        key: telematic[key] for key in REST_SENSOR_KEYS if key in telematic
                    }
                    self._rest_available_by_vin[vin] = True
            self.async_set_updated_data(self._build_payload())

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
        stream_topic = self._entry.options.get(
            CONF_STREAM_TOPIC,
            self._entry.data.get(CONF_STREAM_TOPIC, ""),
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
