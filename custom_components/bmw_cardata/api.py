"""BMW CarData OAuth client."""

from __future__ import annotations

from dataclasses import dataclass
import base64
import hashlib
import secrets
from typing import Any

from aiohttp import ClientResponseError, ClientSession

from .const import (
    API_VERSION,
    API_VERSION_HEADER_NAME,
    BASIC_DATA_PATH_TEMPLATE,
    CARDATA_BASE_URL,
    CONTAINERS_PATH,
    DEVICE_CODE_GRANT_TYPE,
    DEVICE_CODE_PATH,
    DEVICE_CODE_RESPONSE_TYPE,
    MAPPINGS_PATH,
    OAUTH_BASE_URL,
    PKCE_CHALLENGE_METHOD,
    REFRESH_TOKEN_GRANT_TYPE,
    TELEMATIC_DATA_PATH_TEMPLATE,
    TOKEN_PATH,
)


class BmwCarDataAuthError(Exception):
    """Base auth error."""


class BmwCarDataApiError(BmwCarDataAuthError):
    """Unexpected API error."""


class BmwCarDataOAuthError(BmwCarDataAuthError):
    """Expected OAuth error from endpoint."""

    def __init__(self, error: str, description: str | None = None) -> None:
        """Create OAuth error."""
        super().__init__(description or error)
        self.error = error
        self.description = description


@dataclass(slots=True)
class DeviceCodeResponse:
    """Response from device-code initiation."""

    device_code: str
    user_code: str
    verification_uri: str
    expires_in: int
    interval: int


@dataclass(slots=True)
class TokenResponse:
    """Token response from OAuth endpoint."""

    access_token: str
    token_type: str
    expires_in: int
    refresh_token: str
    scope: str | None
    gcid: str | None
    id_token: str | None


@dataclass(slots=True)
class PkcePair:
    """Generated PKCE verifier/challenge pair."""

    code_verifier: str
    code_challenge: str


class BmwCarDataAuthApi:
    """Small HTTP client for BMW Device Code Flow."""

    def __init__(self, session: ClientSession) -> None:
        """Initialize the API wrapper."""
        self._session = session

    @staticmethod
    def generate_pkce_pair() -> PkcePair:
        """Create a PKCE code_verifier and S256 code_challenge."""
        code_verifier = secrets.token_urlsafe(64)
        digest = hashlib.sha256(code_verifier.encode("utf-8")).digest()
        code_challenge = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
        return PkcePair(code_verifier=code_verifier, code_challenge=code_challenge)

    async def request_device_code(
        self,
        *,
        client_id: str,
        scope: str,
        code_challenge: str,
    ) -> DeviceCodeResponse:
        """Initiate the device code flow."""
        payload = {
            "client_id": client_id,
            "response_type": DEVICE_CODE_RESPONSE_TYPE,
            "scope": scope,
            "code_challenge": code_challenge,
            "code_challenge_method": PKCE_CHALLENGE_METHOD,
        }

        data = await self._post_form(DEVICE_CODE_PATH, payload)
        return DeviceCodeResponse(
            device_code=data["device_code"],
            user_code=data["user_code"],
            verification_uri=data["verification_uri"],
            expires_in=int(data.get("expires_in", 0)),
            interval=int(data.get("interval", 5)),
        )

    async def request_token_with_device_code(
        self,
        *,
        client_id: str,
        device_code: str,
        code_verifier: str,
    ) -> TokenResponse:
        """Exchange authorized device code for tokens."""
        payload = {
            "client_id": client_id,
            "device_code": device_code,
            "grant_type": DEVICE_CODE_GRANT_TYPE,
            "code_verifier": code_verifier,
        }

        data = await self._post_form(TOKEN_PATH, payload)
        return TokenResponse(
            access_token=data["access_token"],
            token_type=data["token_type"],
            expires_in=int(data.get("expires_in", 0)),
            refresh_token=data["refresh_token"],
            scope=data.get("scope"),
            gcid=data.get("gcid"),
            id_token=data.get("id_token"),
        )

    async def refresh_token(
        self,
        *,
        client_id: str,
        refresh_token: str,
    ) -> TokenResponse:
        """Refresh access token with refresh token."""
        payload = {
            "grant_type": REFRESH_TOKEN_GRANT_TYPE,
            "refresh_token": refresh_token,
            "client_id": client_id,
        }

        data = await self._post_form(TOKEN_PATH, payload)
        return TokenResponse(
            access_token=data["access_token"],
            token_type=data["token_type"],
            expires_in=int(data.get("expires_in", 0)),
            refresh_token=data["refresh_token"],
            scope=data.get("scope"),
            gcid=data.get("gcid"),
            id_token=data.get("id_token"),
        )

    async def _post_form(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Call form endpoint and map OAuth errors."""
        try:
            async with self._session.post(
                f"{OAUTH_BASE_URL}{path}",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=payload,
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    error = data.get("error") if isinstance(data, dict) else None
                    description = (
                        data.get("error_description") if isinstance(data, dict) else None
                    )
                    if error:
                        raise BmwCarDataOAuthError(error=error, description=description)
                    raise BmwCarDataApiError(
                        f"Unexpected response {response.status} from BMW OAuth endpoint"
                    )
                if not isinstance(data, dict):
                    raise BmwCarDataApiError("Unexpected non-JSON object response")
                return data
        except ClientResponseError as err:
            raise BmwCarDataApiError("HTTP client response error") from err


class BmwCarDataApi:
    """HTTP client for BMW CarData business endpoints."""

    def __init__(self, session: ClientSession) -> None:
        """Initialize CarData API wrapper."""
        self._session = session

    async def get_mappings(self, access_token: str) -> list[dict[str, Any]]:
        """Fetch mapped vehicles for current account."""
        data = await self._get_json(MAPPINGS_PATH, access_token)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        mappings = data.get("mappings")
        if isinstance(mappings, list):
            return [item for item in mappings if isinstance(item, dict)]
        return []

    async def get_basic_data(self, access_token: str, vin: str) -> dict[str, Any]:
        """Fetch basic vehicle data for a VIN."""
        data = await self._get_json(BASIC_DATA_PATH_TEMPLATE.format(vin=vin), access_token)
        if isinstance(data, dict):
            return data
        return {}

    async def get_containers(self, access_token: str) -> list[dict[str, Any]]:
        """Fetch account containers."""
        data = await self._get_json(CONTAINERS_PATH, access_token)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        containers = data.get("containers")
        if isinstance(containers, list):
            return [item for item in containers if isinstance(item, dict)]
        return []

    async def get_telematic_data(
        self,
        access_token: str,
        vin: str,
        container_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Fetch telematic data for a VIN and container."""
        data = await self._get_json(
            TELEMATIC_DATA_PATH_TEMPLATE.format(vin=vin),
            access_token,
            params={"containerId": container_id},
        )
        if not isinstance(data, dict):
            return {}
        telematic_data = data.get("telematicData")
        if not isinstance(telematic_data, dict):
            return {}
        return {
            key: value
            for key, value in telematic_data.items()
            if isinstance(key, str) and isinstance(value, dict)
        }

    async def _get_json(
        self,
        path: str,
        access_token: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        """Perform authorized GET and return JSON payload."""
        headers = {
            "Authorization": f"Bearer {access_token}",
            API_VERSION_HEADER_NAME: API_VERSION,
            "Accept": "application/json",
        }

        try:
            async with self._session.get(
                f"{CARDATA_BASE_URL}{path}", headers=headers, params=params
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    error = None
                    description = None
                    if isinstance(data, dict):
                        error = data.get("error") or data.get("exveErrorId")
                        description = data.get("error_description") or data.get("exveErrorMsg")
                    if error:
                        raise BmwCarDataOAuthError(error=error, description=description)
                    raise BmwCarDataApiError(
                        f"Unexpected response {response.status} from BMW CarData endpoint"
                    )
                if not isinstance(data, (dict, list)):
                    raise BmwCarDataApiError("Unexpected non-JSON object response")
                return data
        except ClientResponseError as err:
            raise BmwCarDataApiError("HTTP client response error") from err

