"""BMW CarData OAuth client."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import secrets
from typing import Any
from urllib.parse import quote, urlencode

from aiohttp import ClientError, ClientSession, ClientTimeout

from .const import (
    DEVICE_CODE_GRANT_TYPE,
    DEVICE_CODE_PATH,
    DEVICE_CODE_RESPONSE_TYPE,
    OAUTH_BASE_URL,
    PKCE_CHALLENGE_METHOD,
    REFRESH_TOKEN_GRANT_TYPE,
    REST_BASE_URL,
    TOKEN_PATH,
)


class BmwCarDataAuthError(Exception):
    """Base auth error."""


class BmwCarDataApiError(BmwCarDataAuthError):
    """Unexpected API error."""


class BmwCarDataRestAuthError(BmwCarDataApiError):
    """REST access is unauthorized."""


class BmwCarDataRateLimitError(BmwCarDataApiError):
    """BMW's daily REST quota has been exhausted."""


class BmwCarDataRestApi:
    """Read vehicle snapshots without creating or changing containers."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session

    async def async_get_telematic_data(
        self, *, access_token: str, vin: str, container_id: str
    ) -> dict[str, dict[str, Any]]:
        """Read the latest stored telematics for a vehicle and container."""
        url = (
            f"{REST_BASE_URL}/customers/vehicles/{quote(vin, safe='')}/telematicData?"
            f"{urlencode({'containerId': container_id})}"
        )
        try:
            async with self._session.get(
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "x-version": "v1",
                    "Accept": "application/json",
                },
                timeout=ClientTimeout(total=30),
            ) as response:
                if response.status in (401, 403):
                    raise BmwCarDataRestAuthError(
                        "BMW REST access denied; check API subscription and authorization"
                    )
                if response.status == 429:
                    raise BmwCarDataRateLimitError("BMW daily REST quota exhausted")
                if response.status >= 400:
                    raise BmwCarDataApiError(
                        f"BMW REST request failed with HTTP {response.status}"
                    )
                data = await response.json(content_type=None)
        except (ClientError, TimeoutError, ValueError) as err:
            raise BmwCarDataApiError("BMW REST request or JSON decoding failed") from err

        telematic = data.get("telematicData") if isinstance(data, dict) else None
        if not isinstance(telematic, dict) or any(
            not isinstance(key, str) or not isinstance(value, dict)
            for key, value in telematic.items()
        ):
            raise BmwCarDataApiError("BMW REST returned invalid telematic data")
        return telematic


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
    """Token response."""

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
    """OAuth client used to authenticate the MQTT connection."""

    def __init__(self, session: ClientSession) -> None:
        """Initialize the OAuth client."""
        self._session = session

    @staticmethod
    def generate_pkce_pair() -> PkcePair:
        """Create a PKCE code verifier and S256 challenge."""
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
        data = await self._post_form(
            DEVICE_CODE_PATH,
            {
                "client_id": client_id,
                "response_type": DEVICE_CODE_RESPONSE_TYPE,
                "scope": scope,
                "code_challenge": code_challenge,
                "code_challenge_method": PKCE_CHALLENGE_METHOD,
            },
        )
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
        """Exchange an authorized device code for tokens."""
        data = await self._post_form(
            TOKEN_PATH,
            {
                "client_id": client_id,
                "device_code": device_code,
                "grant_type": DEVICE_CODE_GRANT_TYPE,
                "code_verifier": code_verifier,
            },
        )
        return self._token_response(data)

    async def refresh_token(
        self,
        *,
        client_id: str,
        refresh_token: str,
    ) -> TokenResponse:
        """Refresh MQTT authentication tokens."""
        data = await self._post_form(
            TOKEN_PATH,
            {
                "grant_type": REFRESH_TOKEN_GRANT_TYPE,
                "refresh_token": refresh_token,
                "client_id": client_id,
            },
        )
        return self._token_response(data)

    @staticmethod
    def _token_response(data: dict[str, Any]) -> TokenResponse:
        """Convert an OAuth payload to a token response."""
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
        """Call an OAuth form endpoint and map errors."""
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
                        data.get("error_description")
                        if isinstance(data, dict)
                        else None
                    )
                    if error:
                        raise BmwCarDataOAuthError(
                            error=error,
                            description=description,
                        )
                    raise BmwCarDataApiError(
                        f"Unexpected response {response.status} from BMW OAuth endpoint"
                    )
                if not isinstance(data, dict):
                    raise BmwCarDataApiError("Unexpected non-JSON object response")
                return data
        except (ClientError, TimeoutError, ValueError) as err:
            raise BmwCarDataApiError("OAuth request or JSON decoding failed") from err
