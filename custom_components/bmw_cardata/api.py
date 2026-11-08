"""BMW CarData OAuth client."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime
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
    REST_SENSOR_KEYS,
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
    """Read vehicle snapshots and manage the integration's data container."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session

    async def async_list_containers(
        self, *, access_token: str
    ) -> list[dict[str, Any]]:
        """List active containers with validated creation timestamps."""
        data = await self._async_request("GET", "/customers/containers", access_token)
        containers = data.get("containers")
        if not isinstance(containers, list):
            raise BmwCarDataApiError("BMW REST returned an invalid container list")
        active = []
        for container in containers:
            if not isinstance(container, dict):
                raise BmwCarDataApiError("BMW REST returned invalid container metadata")
            if container.get("state") not in ("ACTIVE", "INACTIVE", "DELETED"):
                raise BmwCarDataApiError("BMW REST returned an invalid container state")
            if container["state"] != "ACTIVE":
                continue
            self._container_id(container)
            try:
                created = datetime.fromisoformat(container["created"].replace("Z", "+00:00"))
                if created.tzinfo is None:
                    raise ValueError("Container timestamp has no timezone")
            except (KeyError, AttributeError, TypeError, ValueError) as err:
                raise BmwCarDataApiError("BMW REST returned an invalid container timestamp") from err
            active.append(container)
        return active

    async def async_get_container_descriptors(
        self, *, access_token: str, container_id: str
    ) -> list[str]:
        """Read the fields configured in a container."""
        data = await self._async_request(
            "GET", f"/customers/containers/{quote(container_id, safe='')}", access_token
        )
        descriptors = data.get("technicalDescriptors")
        if not isinstance(descriptors, list) or any(
            not isinstance(descriptor, str) for descriptor in descriptors
        ):
            raise BmwCarDataApiError("BMW REST returned invalid container descriptors")
        return descriptors

    async def async_create_container(self, *, access_token: str) -> str:
        """Create a container containing only the three REST sensor fields."""
        data = await self._async_request(
            "POST",
            "/customers/containers",
            access_token,
            payload={
                "name": "Home Assistant BMW CarData",
                "purpose": "Battery level, charging status and charging method sensors",
                "technicalDescriptors": list(REST_SENSOR_KEYS),
            },
        )
        return self._container_id(data)

    @staticmethod
    def _container_id(data: dict[str, Any]) -> str:
        container_id = data.get("containerId")
        if not isinstance(container_id, str) or not container_id.strip():
            raise BmwCarDataApiError("BMW REST returned an invalid container ID")
        return container_id

    async def async_get_telematic_data(
        self, *, access_token: str, vin: str, container_id: str
    ) -> dict[str, dict[str, Any]]:
        """Read the latest stored telematics for a vehicle and container."""
        path = (
            f"/customers/vehicles/{quote(vin, safe='')}/telematicData?"
            f"{urlencode({'containerId': container_id})}"
        )
        data = await self._async_request("GET", path, access_token)
        telematic = data.get("telematicData")
        if not isinstance(telematic, dict) or any(
            not isinstance(key, str) or not isinstance(value, dict)
            for key, value in telematic.items()
        ):
            raise BmwCarDataApiError("BMW REST returned invalid telematic data")
        return telematic

    async def _async_request(
        self, method: str, path: str, access_token: str,
        *, payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Apply common authentication, timeouts and errors to REST requests."""
        try:
            async with self._session.request(
                method, f"{REST_BASE_URL}{path}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "x-version": "v1",
                    "Accept": "application/json",
                },
                timeout=ClientTimeout(total=30),
                json=payload,
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

        if not isinstance(data, dict):
            raise BmwCarDataApiError("BMW REST returned a non-object JSON response")
        return data


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
