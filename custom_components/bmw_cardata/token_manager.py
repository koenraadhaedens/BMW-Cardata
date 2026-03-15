"""Token management for BMW CarData."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_CLIENT_ID
from homeassistant.core import HomeAssistant

from .api import BmwCarDataAuthApi
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_GCID,
    CONF_ID_TOKEN,
    CONF_REFRESH_TOKEN,
    CONF_SCOPE,
    CONF_TOKEN_EXPIRES_AT,
    TOKEN_REFRESH_MARGIN_SECONDS,
)


class BmwCarDataTokenManager:
    """Manages access token lifecycle and persistence."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, auth_api: BmwCarDataAuthApi) -> None:
        """Initialize token manager."""
        self._hass = hass
        self._entry = entry
        self._auth_api = auth_api

    async def async_get_access_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        if not self._token_needs_refresh:
            token = self._entry.data.get(CONF_ACCESS_TOKEN)
            if isinstance(token, str) and token:
                return token

        updated_data = await self._async_refresh_tokens()

        token = updated_data.get(CONF_ACCESS_TOKEN)
        if not isinstance(token, str) or not token:
            raise ValueError("BMW CarData access token missing after refresh")
        return token

    async def async_get_id_token(self) -> str:
        """Return a valid ID token for MQTT streaming authentication."""
        id_token = self._entry.data.get(CONF_ID_TOKEN)
        if not self._token_needs_refresh and isinstance(id_token, str) and id_token:
            return id_token

        updated_data = await self._async_refresh_tokens()
        refreshed_id_token = updated_data.get(CONF_ID_TOKEN)
        if isinstance(refreshed_id_token, str) and refreshed_id_token:
            return refreshed_id_token

        raise ValueError(
            "BMW CarData ID token missing after refresh; re-authentication may be required"
        )

    async def _async_refresh_tokens(self) -> dict:
        """Refresh tokens and persist updated entry data."""
        token_response = await self._auth_api.refresh_token(
            client_id=self._entry.data[CONF_CLIENT_ID],
            refresh_token=self._entry.data[CONF_REFRESH_TOKEN],
        )
        expires_at = datetime.now(tz=UTC) + timedelta(seconds=token_response.expires_in)

        existing_id_token = self._entry.data.get(CONF_ID_TOKEN)
        existing_gcid = self._entry.data.get(CONF_GCID)
        existing_scope = self._entry.data.get(CONF_SCOPE)
        new_id_token = token_response.id_token
        new_gcid = token_response.gcid
        new_scope = token_response.scope
        if not isinstance(new_id_token, str) or not new_id_token:
            new_id_token = existing_id_token
        if not isinstance(new_gcid, str) or not new_gcid:
            new_gcid = existing_gcid
        if not isinstance(new_scope, str) or not new_scope:
            new_scope = existing_scope

        updated_data = {
            **self._entry.data,
            CONF_ACCESS_TOKEN: token_response.access_token,
            CONF_REFRESH_TOKEN: token_response.refresh_token,
            CONF_ID_TOKEN: new_id_token,
            CONF_SCOPE: new_scope,
            CONF_GCID: new_gcid,
            CONF_TOKEN_EXPIRES_AT: expires_at.isoformat(),
        }
        self._hass.config_entries.async_update_entry(self._entry, data=updated_data)
        return updated_data

    @property
    def _token_needs_refresh(self) -> bool:
        """Determine if token is missing or close to expiry."""
        expires_raw = self._entry.data.get(CONF_TOKEN_EXPIRES_AT)
        token = self._entry.data.get(CONF_ACCESS_TOKEN)
        if not isinstance(token, str) or not token:
            return True
        if not isinstance(expires_raw, str):
            return True

        try:
            expires_at = datetime.fromisoformat(expires_raw)
        except ValueError:
            return True

        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)

        refresh_deadline = datetime.now(tz=UTC) + timedelta(
            seconds=TOKEN_REFRESH_MARGIN_SECONDS
        )
        return expires_at <= refresh_deadline
