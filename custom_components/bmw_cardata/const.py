"""Constants for the BMW CarData integration."""

from __future__ import annotations

DOMAIN = "bmw_cardata"
DATA_ENTRIES = "entries"
PLATFORMS = ["sensor", "binary_sensor"]

OAUTH_BASE_URL = "https://customer.bmwgroup.com"
DEVICE_CODE_PATH = "/gcdm/oauth/device/code"
TOKEN_PATH = "/gcdm/oauth/token"
CARDATA_BASE_URL = "https://api-cardata.bmwgroup.com"
MAPPINGS_PATH = "/customers/vehicles/mappings"
BASIC_DATA_PATH_TEMPLATE = "/customers/vehicles/{vin}/basicData"
CONTAINERS_PATH = "/customers/containers"
TELEMATIC_DATA_PATH_TEMPLATE = "/customers/vehicles/{vin}/telematicData"

DEFAULT_SCOPE = "authenticate_user openid cardata:api:read"
STREAMING_SCOPE = "cardata:streaming:read"
DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
DEVICE_CODE_RESPONSE_TYPE = "device_code"
PKCE_CHALLENGE_METHOD = "S256"
REFRESH_TOKEN_GRANT_TYPE = "refresh_token"
API_VERSION_HEADER_NAME = "x-version"
API_VERSION = "v1"
TOKEN_REFRESH_MARGIN_SECONDS = 300
COORDINATOR_UPDATE_INTERVAL_SECONDS = 300

CONF_ACCESS_TOKEN = "access_token"
CONF_REFRESH_TOKEN = "refresh_token"
CONF_ID_TOKEN = "id_token"
CONF_SCOPE = "scope"
CONF_GCID = "gcid"
CONF_TOKEN_EXPIRES_AT = "token_expires_at"
CONF_MAPPINGS = "mappings"
CONF_BASIC_DATA_BY_VIN = "basic_data_by_vin"
CONF_TELEMATIC_DATA_BY_VIN = "telematic_data_by_vin"
CONF_ACTIVE_CONTAINER_ID = "active_container_id"
CONF_USE_STREAMING = "use_streaming"
CONF_STREAM_HOST = "stream_host"
CONF_STREAM_PORT = "stream_port"
CONF_STREAM_TOPIC = "stream_topic"
CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED = "stream_startup_bootstrap_completed"

DEFAULT_USE_STREAMING = False
DEFAULT_STREAM_HOST = "customer.streaming-cardata.bmwgroup.com"
DEFAULT_STREAM_PORT = 9000

SERVICE_REFRESH_DATA = "refresh_data"

ERROR_AUTHORIZATION_PENDING = "authorization_pending"
ERROR_SLOW_DOWN = "slow_down"
ERROR_EXPIRED_TOKEN = "expired_token"
ERROR_ACCESS_DENIED = "access_denied"
ERROR_INVALID_CLIENT = "invalid_client"
ERROR_INVALID_REQUEST = "invalid_request"
