"""BMW CarData integration."""

from __future__ import annotations

from collections.abc import Mapping
from functools import partial

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import BmwCarDataApi, BmwCarDataAuthApi
from .const import DATA_ENTRIES, DOMAIN, PLATFORMS, SERVICE_REFRESH_DATA
from .coordinator import BmwCarDataCoordinator
from .stream_manager import BmwCarDataStreamManager
from .token_manager import BmwCarDataTokenManager


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up BMW CarData from a config entry."""
    domain_data = hass.data.setdefault(DOMAIN, {DATA_ENTRIES: {}})

    session = async_get_clientsession(hass)
    auth_api = BmwCarDataAuthApi(session)
    cardata_api = BmwCarDataApi(session)
    token_manager = BmwCarDataTokenManager(hass=hass, entry=entry, auth_api=auth_api)
    stream_manager = BmwCarDataStreamManager(
        hass,
        entry=entry,
        token_manager=token_manager,
        on_updates=lambda: coordinator.async_apply_stream_snapshot(),
    )
    coordinator = BmwCarDataCoordinator(
        hass,
        entry=entry,
        api=cardata_api,
        token_manager=token_manager,
        stream_manager=stream_manager,
    )
    await stream_manager.async_start()
    await coordinator.async_config_entry_first_refresh()

    domain_data[DATA_ENTRIES][entry.entry_id] = {
        "coordinator": coordinator,
        "token_manager": token_manager,
        "stream_manager": stream_manager,
    }

    if not hass.services.has_service(DOMAIN, SERVICE_REFRESH_DATA):
        hass.services.async_register(
            DOMAIN,
            SERVICE_REFRESH_DATA,
            partial(_async_handle_refresh_service, hass),
        )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not unload_ok:
        return False

    domain_data = hass.data.get(DOMAIN, {})
    entries = domain_data.get(DATA_ENTRIES, {})
    if isinstance(entries, dict):
        entry_data = entries.pop(entry.entry_id, None)
        if isinstance(entry_data, dict):
            stream_manager: BmwCarDataStreamManager | None = entry_data.get("stream_manager")
            if stream_manager is not None:
                await stream_manager.async_stop()

    if not entries:
        if hass.services.has_service(DOMAIN, SERVICE_REFRESH_DATA):
            hass.services.async_remove(DOMAIN, SERVICE_REFRESH_DATA)
        hass.data.pop(DOMAIN)
    return True


async def _async_handle_refresh_service(hass: HomeAssistant, call) -> None:
    """Handle manual refresh service for all configured entries."""
    domain_data = hass.data.get(DOMAIN, {})
    entries: Mapping[str, dict] = domain_data.get(DATA_ENTRIES, {})
    for entry_data in entries.values():
        coordinator: BmwCarDataCoordinator | None = entry_data.get("coordinator")
        if coordinator is not None:
            await coordinator.async_request_refresh()
