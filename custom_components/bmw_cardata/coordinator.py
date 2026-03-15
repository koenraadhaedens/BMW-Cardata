"""Data coordinator for BMW CarData."""

from __future__ import annotations

from datetime import timedelta
import logging
import time
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import BmwCarDataApi, BmwCarDataApiError, BmwCarDataOAuthError
from .const import (
    CONF_ACTIVE_CONTAINER_ID,
    CONF_BASIC_DATA_BY_VIN,
    CONF_MAPPINGS,
    CONF_STREAM_HOST,
    CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED,
    CONF_STREAM_TOPIC,
    CONF_USE_STREAMING,
    CONF_TELEMATIC_DATA_BY_VIN,
    COORDINATOR_UPDATE_INTERVAL_SECONDS,
    ERROR_EXPIRED_TOKEN,
)
from .stream_manager import BmwCarDataStreamManager
from .token_manager import BmwCarDataTokenManager


class BmwCarDataCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinator to fetch and cache BMW CarData payloads."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        entry: ConfigEntry,
        api: BmwCarDataApi,
        token_manager: BmwCarDataTokenManager,
        stream_manager: BmwCarDataStreamManager,
    ) -> None:
        """Initialize coordinator."""
        super().__init__(
            hass,
            logger=logging.getLogger(__name__),
            name="BMW CarData",
            update_interval=timedelta(seconds=COORDINATOR_UPDATE_INTERVAL_SECONDS),
        )
        self._api = api
        self._entry = entry
        self._token_manager = token_manager
        self._stream_manager = stream_manager
        self._container_rotation_index = 0
        self._telematic_cache_by_vin: dict[str, dict[str, dict[str, Any]]] = {}
        self._mappings_cache: list[dict[str, Any]] = []
        self._basic_cache_by_vin: dict[str, dict[str, Any]] = {}
        self._stream_fallback_logged = False
        # In streaming mode, run a one-time REST bootstrap to warm data cache.
        self._stream_bootstrap_completed = False
        self._next_stream_bootstrap_monotonic = 0.0

    @property
    def streaming_requested(self) -> bool:
        """Return whether user requested streaming mode and basic settings are present."""
        enabled = bool(self._entry.options.get(CONF_USE_STREAMING, self._entry.data.get(CONF_USE_STREAMING, False)))
        host = self._entry.options.get(CONF_STREAM_HOST, self._entry.data.get(CONF_STREAM_HOST))
        topic = self._entry.options.get(CONF_STREAM_TOPIC, self._entry.data.get(CONF_STREAM_TOPIC))
        return enabled and isinstance(host, str) and bool(host) and isinstance(topic, str) and bool(topic)

    @property
    def use_streaming(self) -> bool:
        """Return whether streaming can be used right now."""
        return self.streaming_requested and self._stream_manager.enabled

    async def async_apply_stream_snapshot(self) -> None:
        """Apply latest stream cache to coordinator state and update entities."""
        if not self.use_streaming:
            return
        telematic_snapshot = await self._stream_manager.async_get_telematic_snapshot()
        if not telematic_snapshot:
            self.logger.debug("BMW stream snapshot empty; no telematic updates yet")
            return

        self._telematic_cache_by_vin = self._merge_telematic_by_vin(
            self._telematic_cache_by_vin,
            telematic_snapshot,
        )

        base_data = self.data if isinstance(self.data, dict) else {}
        updated = {
            CONF_MAPPINGS: self._build_stream_mappings(base_data.get(CONF_MAPPINGS, [])),
            CONF_BASIC_DATA_BY_VIN: base_data.get(CONF_BASIC_DATA_BY_VIN, {}),
            CONF_TELEMATIC_DATA_BY_VIN: dict(self._telematic_cache_by_vin),
            CONF_ACTIVE_CONTAINER_ID: base_data.get(CONF_ACTIVE_CONTAINER_ID),
            CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED: base_data.get(
                CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED,
                self._stream_bootstrap_completed,
            ),
        }
        summary = {vin: len(values) for vin, values in self._telematic_cache_by_vin.items()}
        self.logger.debug("Applying BMW stream snapshot with key counts per VIN: %s", summary)
        self.async_set_updated_data(updated)

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from API."""
        try:
            if self.streaming_requested and not self.use_streaming and not self._stream_fallback_logged:
                self.logger.warning(
                    "BMW streaming requested but not available (missing scope or stream credentials); "
                    "REST polling is skipped in streaming-only mode until streaming auth is valid."
                )
                self._stream_fallback_logged = True
            if self.streaming_requested and not self.use_streaming:
                return self._build_payload_from_cache(require_existing=False)
            if self.use_streaming:
                return await self._async_fetch_snapshot("")
            access_token = await self._token_manager.async_get_access_token()
            return await self._async_fetch_snapshot(access_token)
        except BmwCarDataOAuthError as err:
            if err.error == ERROR_EXPIRED_TOKEN:
                try:
                    access_token = await self._token_manager.async_get_access_token()
                    return await self._async_fetch_snapshot(access_token)
                except (BmwCarDataApiError, BmwCarDataOAuthError) as retry_err:
                    if self._is_rate_limited_error(retry_err):
                        self.logger.warning("BMW CarData API rate limited during retry; reusing cached data")
                        return self._build_payload_from_cache(require_existing=True)
                    raise UpdateFailed(str(retry_err)) from retry_err
            if self._is_rate_limited_error(err):
                self.logger.warning("BMW CarData API rate limited; reusing cached data")
                return self._build_payload_from_cache(require_existing=True)
            raise UpdateFailed(str(err)) from err
        except BmwCarDataApiError as err:
            if self._is_rate_limited_error(err):
                self.logger.warning("BMW CarData API rate limited; reusing cached data")
                return self._build_payload_from_cache(require_existing=True)
            raise UpdateFailed(str(err)) from err

    async def _async_fetch_snapshot(self, access_token: str, *, force_rest: bool = False) -> dict[str, Any]:
        """Fetch one update snapshot with bounded API pressure."""
        active_container_id: str | None = None
        if self.use_streaming and not force_rest:
            await self._stream_manager.async_start()
            telematic_data_by_vin = await self._stream_manager.async_get_telematic_snapshot()
            telematic_data_by_vin = self._merge_telematic_by_vin(
                self._telematic_cache_by_vin,
                telematic_data_by_vin,
            )

            if self._should_run_stream_bootstrap():
                try:
                    bootstrap_access_token = await self._token_manager.async_get_access_token()
                    bootstrap_payload = await self._async_fetch_snapshot(
                        bootstrap_access_token,
                        force_rest=True,
                    )

                    bootstrap_mappings = bootstrap_payload.get(CONF_MAPPINGS, [])
                    if isinstance(bootstrap_mappings, list):
                        self._mappings_cache = [item for item in bootstrap_mappings if isinstance(item, dict)]

                    bootstrap_basic = bootstrap_payload.get(CONF_BASIC_DATA_BY_VIN, {})
                    if isinstance(bootstrap_basic, dict):
                        for vin, basic_data in bootstrap_basic.items():
                            if isinstance(vin, str) and isinstance(basic_data, dict):
                                self._basic_cache_by_vin[vin] = basic_data

                    bootstrap_telematic = bootstrap_payload.get(CONF_TELEMATIC_DATA_BY_VIN, {})
                    if isinstance(bootstrap_telematic, dict):
                        telematic_data_by_vin = self._merge_telematic_by_vin(
                            telematic_data_by_vin,
                            bootstrap_telematic,
                        )

                    self._stream_bootstrap_completed = True
                    self.logger.debug(
                        "BMW stream startup bootstrap via REST completed; further REST bootstraps disabled"
                    )
                except (BmwCarDataApiError, BmwCarDataOAuthError) as err:
                    # Keep streaming alive and avoid frequent retries on bootstrap failure.
                    self._next_stream_bootstrap_monotonic = time.monotonic() + 300
                    if self._is_rate_limited_error(err):
                        self.logger.warning(
                            "BMW stream startup bootstrap REST call rate limited; retry in 300s while keeping stream data"
                        )
                    else:
                        self.logger.warning(
                            "BMW stream startup bootstrap REST call failed; retry in 300s while keeping stream data: %s",
                            err,
                        )

            self._telematic_cache_by_vin = telematic_data_by_vin
            mappings = self._build_stream_mappings(self._mappings_cache)
            basic_data_by_vin = dict(self._basic_cache_by_vin)
            return {
                CONF_MAPPINGS: mappings,
                CONF_BASIC_DATA_BY_VIN: basic_data_by_vin,
                CONF_TELEMATIC_DATA_BY_VIN: telematic_data_by_vin,
                CONF_ACTIVE_CONTAINER_ID: None,
                CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED: self._stream_bootstrap_completed,
            }

        mappings = await self._api.get_mappings(access_token)
        if isinstance(mappings, list):
            self._mappings_cache = [item for item in mappings if isinstance(item, dict)]

        if not self.use_streaming:
            containers = await self._api.get_containers(access_token)
            active_container_ids = self._select_active_container_ids(containers)
            active_container_id = self._select_container_for_this_cycle(active_container_ids)

        basic_data_by_vin: dict[str, dict[str, Any]] = {}
        telematic_data_by_vin = dict(self._telematic_cache_by_vin)

        rate_limited_on_telematic = False
        for mapping in mappings:
            vin = mapping.get("vin")
            mapping_type = str(mapping.get("mappingType", "")).upper()
            if not isinstance(vin, str) or not vin:
                continue
            if mapping_type and mapping_type != "PRIMARY":
                continue

            basic_data_by_vin[vin] = await self._api.get_basic_data(access_token, vin)
            self._basic_cache_by_vin[vin] = basic_data_by_vin[vin]
            if not active_container_id:
                continue

            try:
                container_telematic = await self._api.get_telematic_data(
                    access_token,
                    vin,
                    active_container_id,
                )
            except (BmwCarDataApiError, BmwCarDataOAuthError) as err:
                if self._is_rate_limited_error(err):
                    rate_limited_on_telematic = True
                    break
                raise

            existing_telematic = telematic_data_by_vin.get(vin, {})
            if not isinstance(existing_telematic, dict):
                existing_telematic = {}
            telematic_data_by_vin[vin] = self._merge_telematic_entries(
                existing_telematic,
                container_telematic,
            )

        self._telematic_cache_by_vin = telematic_data_by_vin
        if rate_limited_on_telematic:
            self.logger.warning(
                "BMW CarData API rate limited during telematic fetch; keeping cached telematics"
            )

        return {
            CONF_MAPPINGS: mappings,
            CONF_BASIC_DATA_BY_VIN: basic_data_by_vin,
            CONF_TELEMATIC_DATA_BY_VIN: telematic_data_by_vin,
            CONF_ACTIVE_CONTAINER_ID: active_container_id,
            CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED: self._stream_bootstrap_completed,
        }

    def _should_run_stream_bootstrap(self) -> bool:
        """Return whether startup REST bootstrap should run in streaming mode."""
        return (
            not self._stream_bootstrap_completed
            and time.monotonic() >= self._next_stream_bootstrap_monotonic
        )

    def _build_stream_mappings(self, existing_mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Build mapping list from cached mappings and VINs present in stream/topic."""
        mappings_by_vin: dict[str, dict[str, Any]] = {}

        if isinstance(existing_mappings, list):
            for item in existing_mappings:
                if not isinstance(item, dict):
                    continue
                vin = item.get("vin")
                if isinstance(vin, str) and vin:
                    mappings_by_vin[vin] = item

        stream_topic = self._entry.options.get(CONF_STREAM_TOPIC, self._entry.data.get(CONF_STREAM_TOPIC, ""))
        if isinstance(stream_topic, str):
            for segment in stream_topic.split("/"):
                stripped = segment.strip().upper()
                if len(stripped) == 17 and stripped.isalnum():
                    mappings_by_vin.setdefault(stripped, {"vin": stripped, "mappingType": "PRIMARY"})

        for vin in self._telematic_cache_by_vin:
            if isinstance(vin, str) and vin:
                mappings_by_vin.setdefault(vin, {"vin": vin, "mappingType": "PRIMARY"})

        return list(mappings_by_vin.values())

    @staticmethod
    def _merge_telematic_by_vin(
        base: dict[str, dict[str, dict[str, Any]]],
        incoming: dict[str, dict[str, dict[str, Any]]],
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """Merge telematic entries per VIN."""
        merged = dict(base)
        for vin, telematic in incoming.items():
            if not isinstance(vin, str) or not isinstance(telematic, dict):
                continue
            existing = merged.get(vin, {})
            if not isinstance(existing, dict):
                existing = {}
            vin_merged = dict(existing)
            for key, entry in telematic.items():
                if isinstance(key, str) and isinstance(entry, dict):
                    vin_merged[key] = entry
            merged[vin] = vin_merged
        return merged

    def _build_payload_from_cache(self, require_existing: bool = False) -> dict[str, Any]:
        """Return latest known payload, allowing setup to continue on throttling."""
        if self.data and isinstance(self.data, dict):
            return self.data
        if require_existing:
            raise UpdateFailed("BMW CarData API rate limited. Retry after cooldown window.")
        return {
            CONF_MAPPINGS: [],
            CONF_BASIC_DATA_BY_VIN: {},
            CONF_TELEMATIC_DATA_BY_VIN: dict(self._telematic_cache_by_vin),
            CONF_ACTIVE_CONTAINER_ID: None,
            CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED: self._stream_bootstrap_completed,
        }

    def _select_container_for_this_cycle(self, active_container_ids: list[str]) -> str | None:
        """Select one active container per cycle to limit request volume."""
        if not active_container_ids:
            return None
        index = self._container_rotation_index % len(active_container_ids)
        self._container_rotation_index += 1
        return active_container_ids[index]

    @staticmethod
    def _is_rate_limited_error(err: Exception) -> bool:
        """Determine if an error indicates API throttling/rate limit."""
        if isinstance(err, BmwCarDataOAuthError):
            error_text = (err.error or "").lower()
            description_text = (err.description or "").lower()
            return "rate" in error_text or "limit" in error_text or "rate" in description_text
        return "rate" in str(err).lower() and "limit" in str(err).lower()

    @staticmethod
    def _select_active_container_ids(containers: list[dict[str, Any]]) -> list[str]:
        """Select all active container ids."""
        active_container_ids: list[str] = []
        for container in containers:
            container_id = container.get("containerId")
            state = str(container.get("state", "")).upper()
            if isinstance(container_id, str) and container_id and state == "ACTIVE":
                active_container_ids.append(container_id)
        return active_container_ids

    @staticmethod
    def _merge_telematic_entries(
        target: dict[str, dict[str, Any]],
        source: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Merge telematic entries and prefer newer timestamps for duplicate keys."""
        merged = dict(target)
        for key, source_entry in source.items():
            if key not in merged:
                merged[key] = source_entry
                continue

            existing_entry = merged[key]
            if not isinstance(existing_entry, dict):
                merged[key] = source_entry
                continue

            source_ts = source_entry.get("timestamp")
            existing_ts = existing_entry.get("timestamp")
            if isinstance(source_ts, str) and isinstance(existing_ts, str):
                if source_ts > existing_ts:
                    merged[key] = source_entry
                continue

            if source_entry.get("value") not in (None, ""):
                merged[key] = source_entry

        return merged
