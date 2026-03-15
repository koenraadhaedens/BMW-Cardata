"""Config flow for BMW CarData."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_CLIENT_ID
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import BmwCarDataApiError, BmwCarDataAuthApi, BmwCarDataOAuthError
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_GCID,
    CONF_ID_TOKEN,
    CONF_REFRESH_TOKEN,
    CONF_SCOPE,
    CONF_STREAM_HOST,
    CONF_STREAM_PORT,
    CONF_STREAM_TOPIC,
    CONF_TOKEN_EXPIRES_AT,
    CONF_USE_STREAMING,
    DEFAULT_STREAM_HOST,
    DEFAULT_STREAM_PORT,
    DEFAULT_USE_STREAMING,
    DEFAULT_SCOPE,
    DOMAIN,
    ERROR_ACCESS_DENIED,
    ERROR_AUTHORIZATION_PENDING,
    ERROR_EXPIRED_TOKEN,
    ERROR_INVALID_CLIENT,
    ERROR_INVALID_REQUEST,
    ERROR_SLOW_DOWN,
    STREAMING_SCOPE,
)

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_CLIENT_ID): str,
        vol.Optional(CONF_USE_STREAMING, default=DEFAULT_USE_STREAMING): bool,
        vol.Optional(CONF_STREAM_TOPIC, default=""): str,
    }
)

STEP_AUTHORIZE_DATA_SCHEMA = vol.Schema({})


class BmwCarDataConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for BMW CarData."""

    VERSION = 1

    _client_id: str | None = None
    _device_code: str | None = None
    _code_verifier: str | None = None
    _user_code: str | None = None
    _verification_uri: str | None = None
    _expires_in: int | None = None
    _stream_options: dict[str, Any] | None = None

    async def async_step_user(self, user_input: dict[str, Any] | None = None):
        """Ask for the BMW CarData client ID and start Device Code flow."""
        errors: dict[str, str] = {}

        if user_input is not None:
            client_id = user_input[CONF_CLIENT_ID].strip()
            use_streaming = bool(user_input.get(CONF_USE_STREAMING, DEFAULT_USE_STREAMING))
            stream_topic = str(user_input.get(CONF_STREAM_TOPIC, "")).strip()
            if use_streaming and not stream_topic:
                errors[CONF_STREAM_TOPIC] = "required"

            if errors:
                return self.async_show_form(
                    step_id="user",
                    data_schema=STEP_USER_DATA_SCHEMA,
                    errors=errors,
                )

            self._stream_options = {
                CONF_USE_STREAMING: use_streaming,
                CONF_STREAM_HOST: DEFAULT_STREAM_HOST,
                CONF_STREAM_PORT: DEFAULT_STREAM_PORT,
                CONF_STREAM_TOPIC: stream_topic,
            }
            api = BmwCarDataAuthApi(async_get_clientsession(self.hass))
            pkce = api.generate_pkce_pair()

            try:
                requested_scope = DEFAULT_SCOPE
                if use_streaming and STREAMING_SCOPE not in requested_scope:
                    requested_scope = f"{requested_scope} {STREAMING_SCOPE}"
                device_response = await api.request_device_code(
                    client_id=client_id,
                    scope=requested_scope,
                    code_challenge=pkce.code_challenge,
                )
            except BmwCarDataOAuthError as err:
                errors["base"] = self._oauth_error_to_flow_error(err.error)
            except BmwCarDataApiError:
                _LOGGER.exception("BMW CarData device-code request failed")
                errors["base"] = "cannot_connect"
            else:
                self._client_id = client_id
                self._device_code = device_response.device_code
                self._code_verifier = pkce.code_verifier
                self._user_code = device_response.user_code
                self._verification_uri = device_response.verification_uri
                self._expires_in = device_response.expires_in
                return await self.async_step_authorize()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_authorize(self, user_input: dict[str, Any] | None = None):
        """Show BMW user code and exchange device code after user authorization."""
        errors: dict[str, str] = {}

        if user_input is not None:
            if not all([self._client_id, self._device_code, self._code_verifier]):
                return self.async_abort(reason="missing_flow_state")

            api = BmwCarDataAuthApi(async_get_clientsession(self.hass))
            try:
                token_response = await api.request_token_with_device_code(
                    client_id=self._client_id,
                    device_code=self._device_code,
                    code_verifier=self._code_verifier,
                )
            except BmwCarDataOAuthError as err:
                errors["base"] = self._oauth_error_to_flow_error(err.error)
            except BmwCarDataApiError:
                _LOGGER.exception("BMW CarData token request failed")
                errors["base"] = "cannot_connect"
            else:
                unique_id = token_response.gcid or self._client_id
                await self.async_set_unique_id(unique_id)
                self._abort_if_unique_id_configured(updates={CONF_CLIENT_ID: self._client_id})

                expires_at = datetime.now(tz=UTC) + timedelta(
                    seconds=token_response.expires_in
                )
                entry_data = {
                    CONF_CLIENT_ID: self._client_id,
                    CONF_ACCESS_TOKEN: token_response.access_token,
                    CONF_REFRESH_TOKEN: token_response.refresh_token,
                    CONF_ID_TOKEN: token_response.id_token,
                    CONF_SCOPE: token_response.scope,
                    CONF_GCID: token_response.gcid,
                    CONF_TOKEN_EXPIRES_AT: expires_at.isoformat(),
                    **(self._stream_options or {}),
                }

                title = f"BMW CarData ({token_response.gcid})" if token_response.gcid else "BMW CarData"
                return self.async_create_entry(title=title, data=entry_data)

        verification_uri_complete = (
            f"{self._verification_uri}?user_code={self._user_code}"
            if self._verification_uri and self._user_code
            else ""
        )

        return self.async_show_form(
            step_id="authorize",
            data_schema=STEP_AUTHORIZE_DATA_SCHEMA,
            errors=errors,
            description_placeholders={
                "user_code": self._user_code or "",
                "verification_uri": self._verification_uri or "",
                "verification_uri_complete": verification_uri_complete,
                "expires_in": str(self._expires_in or 0),
            },
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        """Return options flow handler."""
        return BmwCarDataOptionsFlow(config_entry)

    def _oauth_error_to_flow_error(self, error: str) -> str:
        """Map BMW OAuth errors to config flow error keys."""
        known = {
            ERROR_AUTHORIZATION_PENDING,
            ERROR_SLOW_DOWN,
            ERROR_EXPIRED_TOKEN,
            ERROR_ACCESS_DENIED,
            ERROR_INVALID_CLIENT,
            ERROR_INVALID_REQUEST,
        }
        if error in known:
            return error
        return "unknown_oauth_error"


class BmwCarDataOptionsFlow(config_entries.OptionsFlow):
    """BMW CarData options flow (placeholder for future options)."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None):
        """Manage options."""
        errors: dict[str, str] = {}
        if user_input is not None:
            use_streaming = bool(user_input.get(CONF_USE_STREAMING, False))
            stream_topic = str(user_input.get(CONF_STREAM_TOPIC, "")).strip()
            if use_streaming and not stream_topic:
                errors[CONF_STREAM_TOPIC] = "required"

            if not errors:
                return self.async_create_entry(
                    title="",
                    data={
                        CONF_USE_STREAMING: use_streaming,
                        CONF_STREAM_HOST: DEFAULT_STREAM_HOST,
                        CONF_STREAM_PORT: DEFAULT_STREAM_PORT,
                        CONF_STREAM_TOPIC: stream_topic,
                    },
                )

        current_options = {**self.config_entry.options}
        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_USE_STREAMING,
                    default=current_options.get(CONF_USE_STREAMING, DEFAULT_USE_STREAMING),
                ): bool,
                vol.Optional(
                    CONF_STREAM_TOPIC,
                    default=current_options.get(CONF_STREAM_TOPIC, ""),
                ): str,
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema, errors=errors)
