"""Constants for the BMW CarData integration."""

from __future__ import annotations

DOMAIN = "bmw_cardata"
DATA_ENTRIES = "entries"
PLATFORMS = ["sensor", "binary_sensor", "device_tracker"]

OAUTH_BASE_URL = "https://customer.bmwgroup.com"
DEVICE_CODE_PATH = "/gcdm/oauth/device/code"
TOKEN_PATH = "/gcdm/oauth/token"
DEFAULT_SCOPE = "authenticate_user openid cardata:streaming:read"
STREAMING_SCOPE = "cardata:streaming:read"
REST_SCOPE = "cardata:api:read"
REST_BASE_URL = "https://api-cardata.bmwgroup.com"
REST_UPDATE_HOURS = 2
REST_DAILY_CALL_LIMIT = 50
CONF_REST_CONTAINER_ID = "rest_container_id"
CONF_REST_DATA_BY_VIN = "rest_data_by_vin"
CONF_REST_AVAILABLE_BY_VIN = "rest_available_by_vin"
CONF_REST_REQUEST_STATE = "rest_request_state"
REST_BATTERY_LEVEL = "vehicle.drivetrain.electricEngine.charging.level"
REST_CHARGING_STATUS = "vehicle.drivetrain.electricEngine.charging.status"
REST_CHARGING_METHOD = "vehicle.drivetrain.electricEngine.charging.method"
REST_SENSOR_KEYS = (REST_BATTERY_LEVEL, REST_CHARGING_STATUS, REST_CHARGING_METHOD)
DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
DEVICE_CODE_RESPONSE_TYPE = "device_code"
PKCE_CHALLENGE_METHOD = "S256"
REFRESH_TOKEN_GRANT_TYPE = "refresh_token"
TOKEN_REFRESH_MARGIN_SECONDS = 300

CONF_ACCESS_TOKEN = "access_token"
CONF_REFRESH_TOKEN = "refresh_token"
CONF_ID_TOKEN = "id_token"
CONF_SCOPE = "scope"
CONF_GCID = "gcid"
CONF_TOKEN_EXPIRES_AT = "token_expires_at"
CONF_MAPPINGS = "mappings"
CONF_BASIC_DATA_BY_VIN = "basic_data_by_vin"
CONF_TELEMATIC_DATA_BY_VIN = "telematic_data_by_vin"
CONF_STREAM_HOST = "stream_host"
CONF_STREAM_PORT = "stream_port"
CONF_STREAM_TOPIC = "stream_topic"

DEFAULT_STREAM_HOST = "customer.streaming-cardata.bmwgroup.com"
DEFAULT_STREAM_PORT = 9000
DEFAULT_VERBOSE_LOGGING = False

CONF_VERBOSE_LOGGING = "verbose_logging"

ERROR_AUTHORIZATION_PENDING = "authorization_pending"
ERROR_SLOW_DOWN = "slow_down"
ERROR_EXPIRED_TOKEN = "expired_token"
ERROR_ACCESS_DENIED = "access_denied"
ERROR_INVALID_CLIENT = "invalid_client"
ERROR_INVALID_REQUEST = "invalid_request"
