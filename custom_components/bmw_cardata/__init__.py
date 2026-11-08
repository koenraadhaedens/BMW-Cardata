"""BMW CarData integration."""

from __future__ import annotations

import logging
import logging.handlers
import os

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import BmwCarDataAuthApi, BmwCarDataRestApi
from .const import (
    CONF_REST_CONTAINER_ID,
    CONF_STREAM_TOPIC,
    CONF_USE_REST,
    CONF_VERBOSE_LOGGING,
    DATA_ENTRIES,
    DEFAULT_VERBOSE_LOGGING,
    DEFAULT_STREAM_TOPIC,
    DOMAIN,
    PLATFORMS,
)
from .coordinator import BmwCarDataCoordinator
from .stream_manager import BmwCarDataStreamManager
from .token_manager import BmwCarDataTokenManager

_INTEGRATION_LOGGER_NAME = "custom_components.bmw_cardata"
_FILE_HANDLER_MARKER = "_bmw_cardata_verbose_handler"


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Replace legacy manual container settings without changing entity IDs."""
    if entry.version > 2:
        logging.getLogger(__name__).error("Cannot downgrade a newer BMW CarData entry")
        return False
    if entry.version == 1:
        data = dict(entry.data)
        options = dict(entry.options)
        legacy_id = options.get(CONF_REST_CONTAINER_ID, data.get(CONF_REST_CONTAINER_ID, ""))
        data[CONF_USE_REST] = bool(legacy_id)
        data[CONF_STREAM_TOPIC] = data.get(CONF_STREAM_TOPIC) or DEFAULT_STREAM_TOPIC
        data.pop(CONF_REST_CONTAINER_ID, None)
        options.pop(CONF_REST_CONTAINER_ID, None)
        options[CONF_USE_REST] = data[CONF_USE_REST]
        if CONF_STREAM_TOPIC in options:
            options[CONF_STREAM_TOPIC] = options[CONF_STREAM_TOPIC] or DEFAULT_STREAM_TOPIC
        hass.config_entries.async_update_entry(entry, data=data, options=options, version=2)
    return True


def _apply_verbose_logging(config_dir: str) -> None:
    """Set integration logger to DEBUG and attach a rotating file handler."""
    logger = logging.getLogger(_INTEGRATION_LOGGER_NAME)
    _remove_verbose_logging()  # ensure no duplicate handlers
    logger.setLevel(logging.DEBUG)
    log_path = os.path.join(config_dir, "bmw_cardata.log")
    handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,  # 5 MB per file
        backupCount=3,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(name)s  %(levelname)-8s  %(message)s")
    )
    setattr(handler, _FILE_HANDLER_MARKER, True)
    logger.addHandler(handler)
    logger.debug(
        "BMW CarData verbose logging enabled — writing to %s", log_path
    )


def _remove_verbose_logging() -> None:
    """Remove the BMW CarData file handler and restore propagated log level."""
    logger = logging.getLogger(_INTEGRATION_LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _FILE_HANDLER_MARKER, False):
            logger.removeHandler(handler)
            handler.close()
    # Reset to NOTSET so HA's own logger configuration controls the effective level.
    logger.setLevel(logging.NOTSET)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up BMW CarData from a config entry."""
    # Apply or remove verbose logging based on the current option.
    verbose = bool(
        entry.options.get(
            CONF_VERBOSE_LOGGING,
            entry.data.get(CONF_VERBOSE_LOGGING, DEFAULT_VERBOSE_LOGGING),
        )
    )
    if verbose:
        await hass.async_add_executor_job(_apply_verbose_logging, hass.config.config_dir)
    else:
        await hass.async_add_executor_job(_remove_verbose_logging)

    domain_data = hass.data.setdefault(DOMAIN, {DATA_ENTRIES: {}})

    session = async_get_clientsession(hass)
    auth_api = BmwCarDataAuthApi(session)
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
        stream_manager=stream_manager,
        rest_api=BmwCarDataRestApi(session),
        token_manager=token_manager,
    )
    await coordinator.async_initialize()

    domain_data[DATA_ENTRIES][entry.entry_id] = {
        "coordinator": coordinator,
        "token_manager": token_manager,
        "stream_manager": stream_manager,
        "options": dict(entry.options),
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return True


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Apply changes to REST and MQTT options."""
    runtime = hass.data[DOMAIN][DATA_ENTRIES][entry.entry_id]
    if runtime["options"] != dict(entry.options):
        await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    await hass.async_add_executor_job(_remove_verbose_logging)
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
        hass.data.pop(DOMAIN)
    return True
