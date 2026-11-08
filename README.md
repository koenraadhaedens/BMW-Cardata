# BMW-Cardata
# BMW CarData Home Assistant Integration (HACS)

Custom Home Assistant integration for BMW CarData authentication using OAuth2 Device Code Flow.

## What this does

- Adds a Home Assistant config flow for BMW CarData.
- Asks the user for a BMW CarData **Client ID**.
- Requests a **device code** from BMW OAuth.
- Displays the **user code** and verification URL.
- Exchanges the authorized device code for BMW tokens and stores them in the config entry.
- Automatically refreshes expired/expiring access tokens using the refresh token.
- Creates sensors, binary sensors, and device trackers from incoming MQTT telematics.
- Optionally reads battery level, charging status and charging method through BMW CarData REST.
- Sends no remote vehicle commands and does not create or modify REST containers.

## Install with HACS

1. In HACS, open **Integrations**.
2. Add this repository as a **Custom repository**.
3. Select category **Integration**.
4. Install **BMW CarData**.
5. Restart Home Assistant.

## Setup in Home Assistant

1. Go to **Settings → Devices & Services → Add Integration**.
2. Search for **BMW CarData**.
3. Enter your Client ID and MQTT stream topic from My BMW → CarData.
4. Follow the displayed authorization instructions (user code + URL).
5. Submit once authorized.

## Notes

- MQTT entities remain unknown until their values arrive through the stream.
- Optional REST sensors fetch a startup snapshot, then refresh every two hours.
- You must subscribe your client to `cardata:streaming:read` before authorizing.

## MQTT streaming

The integration uses MQTT for all values except the three optional REST sensors:

1. Open integration options in Home Assistant.
2. Set the stream topic from BMW CarData streaming credentials.
3. Stream host and port use BMW defaults automatically (`customer.streaming-cardata.bmwgroup.com:9000`).
4. The integration uses your BMW `gcid` as MQTT username and token as MQTT password.

If streaming was not enabled during initial setup, remove and re-add the integration with
streaming enabled. BMW must issue a new token containing the
`cardata:streaming:read` scope; changing the option alone cannot add that scope.

Existing entries authorized without `cardata:streaming:read` must be removed and re-added.

## Optional REST battery and charging sensors

1. Subscribe your BMW CarData client to `cardata:api:read`.
2. In integration options, enter your existing REST container ID. The container must
   include `vehicle.drivetrain.electricEngine.charging.level`,
   `vehicle.drivetrain.electricEngine.charging.status`, and
   `vehicle.drivetrain.electricEngine.charging.method`.
3. Complete the reauthorization prompted by Home Assistant if your existing token
   lacks REST access. Existing entity IDs are preserved.

Battery Level and Charging Status then use REST exclusively, and a Charging Method
sensor is added. All other sensors, binary sensors and trackers continue to use MQTT.
Clear the container ID to return to MQTT-only operation.

One telematics request supplies all three REST sensors. Requests are spaced at least
two hours apart per discovered VIN, including across restarts and option reloads:
normally **12 data requests/day for one vehicle**, versus BMW's shared **50/day** quota.
Vehicle VINs are discovered from the stream topic or incoming MQTT data; no vehicle
listing or container-management requests are needed. Additional vehicles consume
additional calls. The integration persists a local daily request count and stops at
50 requests, but cannot count requests made by other apps. HTTP 429 pauses REST until
the next UTC day; MQTT continues normally. Failed REST reads mark only the REST
sensors unavailable, and no immediate retries consume extra calls.

REST readings are BMW's latest stored values, not live vehicle commands. Each REST
sensor exposes the field's BMW timestamp. Missing values remain unknown.

## Tests

With `aiohttp` and `voluptuous` available, run `python -m unittest discover -s tests -v`.
The focused tests mock Home Assistant interfaces; they do not start Home Assistant
or call BMW services.
