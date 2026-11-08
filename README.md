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
- Adds Total Remaining Range from MQTT `vehicle.drivetrain.lastRemainingRange`:
  the vehicle's last reported combined electric + fuel range for plug-in hybrids.
  It remains unknown until BMW publishes the field and uses no extra REST calls.
- Optionally reads battery level, charging status and charging method through BMW CarData REST.
- Automatically reuses the newest compatible active REST container, or creates a
  minimal container when none is compatible. Existing containers are never changed or deleted.
- Sends no remote vehicle commands.

## Install with HACS

1. In HACS, open **Integrations**.
2. Add this repository as a **Custom repository**.
3. Select category **Integration**.
4. Install **BMW CarData**.
5. Restart Home Assistant.

## Setup in Home Assistant

1. Go to **Settings → Devices & Services → Add Integration**.
2. Search for **BMW CarData**.
3. Enter your Client ID. MQTT uses `#` automatically for all vehicles; no topic or
   container ID is required. REST sensors are enabled by default for new setups;
   disable them if you want MQTT only.
4. Follow the displayed authorization instructions (user code + URL).
5. Submit once authorized.

## Notes

- MQTT entities remain unknown until their values arrive through the stream.
- Optional REST sensors fetch a startup snapshot, then refresh every two hours.
- You must subscribe your client to `cardata:streaming:read` before authorizing.

## MQTT streaming

The integration uses MQTT for all values except the three optional REST sensors:

1. Open integration options in Home Assistant.
2. The stream topic defaults to `#` (subscribed as `<gcid>/#`), discovering all vehicles
   from incoming messages. A narrower topic can optionally be set in integration options.
3. Stream host and port use BMW defaults automatically (`customer.streaming-cardata.bmwgroup.com:9000`).
4. The integration uses your BMW `gcid` as MQTT username and token as MQTT password.

The MQTT listener runs as an entry-owned background task: it does not hold up Home
Assistant startup and is cancelled when the integration unloads or Home Assistant
shuts down. With the default `#` topic, mapped vehicles remain zero until BMW sends
the first vehicle message after a restart.

If streaming was not enabled during initial setup, remove and re-add the integration with
streaming enabled. BMW must issue a new token containing the
`cardata:streaming:read` scope; changing the option alone cannot add that scope.

Existing entries authorized without `cardata:streaming:read` must be removed and re-added.

## Optional REST battery and charging sensors

1. Subscribe your BMW CarData client to `cardata:api:read`.
2. Enable REST sensors in integration options. The integration chooses the newest
   active container containing all three required fields:
   `vehicle.drivetrain.electricEngine.charging.level`,
   `vehicle.drivetrain.electricEngine.charging.status`, and
   `vehicle.drivetrain.electricEngine.charging.method`.
   If no compatible container exists, it creates one containing only these fields.
   You do not need to find or enter a container ID.
3. Complete the reauthorization prompted by Home Assistant if your existing token
   lacks REST access. Existing entity IDs are preserved.

Battery Level and Charging Status then use REST exclusively, and a Charging Method
sensor is added. All other sensors, binary sensors and trackers continue to use MQTT.
Disable REST sensors to return to MQTT-only operation. Existing beta installations
with a container ID automatically migrate to automatic selection, preserving their
REST on/off setting, MQTT topic, entity IDs and request count. MQTT-only installations
remain MQTT-only.

One telematics request supplies all three REST sensors. Requests are spaced at least
two hours apart per discovered VIN, including across restarts and option reloads:
normally **12 data requests/day for one vehicle**, versus BMW's shared **50/day** quota.
Container selection is cached across restarts and checked once per UTC day when a
vehicle needs a snapshot. If the newest active container is compatible, discovery
adds two calls (list and details), for normally **14 total REST calls/day** for one
vehicle. Finding an older compatible container adds a details call per candidate.
With no active containers, initial setup uses one list call and one creation call.
BMW allows at most 10 containers; creation errors are logged, and existing containers
are never deleted to make room. If all 10 active containers are incompatible, REST
container checks pause until the next UTC day rather than repeatedly spending quota.

Vehicle VINs are discovered from the stream topic or incoming MQTT data; no vehicle
listing request is needed. Additional vehicles consume additional data calls, but
share the same selected container. Discovery, creation and failed calls all count
toward the integration's persisted local daily limit, which stops at
50 requests, but cannot count requests made by other apps. HTTP 429 pauses REST until
the next UTC day; MQTT continues normally. Failed REST reads mark only the REST
sensors unavailable, and no immediate retries consume extra calls.

REST readings are BMW's latest stored values, not live vehicle commands. Each REST
sensor exposes the field's BMW timestamp. Missing values remain unknown.

## Tests

With `aiohttp` and `voluptuous` available, run `python -m unittest discover -s tests -v`.
The focused tests mock Home Assistant interfaces; they do not start Home Assistant
or call BMW services.
