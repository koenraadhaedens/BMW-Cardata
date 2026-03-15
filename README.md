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
- Creates sensors for mapped vehicles and VIN-level basic diagnostics.
- Creates binary sensors for VIN capabilities (telematics capable, navigation, sun roof).
- Supports optional MQTT streaming mode for telematics updates.

## Install with HACS

1. In HACS, open **Integrations**.
2. Add this repository as a **Custom repository**.
3. Select category **Integration**.
4. Install **BMW CarData**.
5. Restart Home Assistant.

## Setup in Home Assistant

1. Go to **Settings → Devices & Services → Add Integration**.
2. Search for **BMW CarData**.
3. Enter your Client ID from My BMW → CarData.
4. Follow the displayed authorization instructions (user code + URL).
5. Submit once authorized.

## Notes

- This implementation includes `sensor` entities based on mappings/basic data.
- A manual refresh service is available as `bmw_cardata.refresh_data`.
- Battery/fuel/doors telematics entities depend on having an ACTIVE BMW container with relevant keys.
- You must subscribe your client to `cardata:api:read` before authorizing.

## Optional streaming mode (MQTT)

You can enable streaming to reduce REST telematics calls and receive push updates:

1. Open integration options in Home Assistant.
2. Enable **Use MQTT streaming**.
3. Set stream topic from BMW CarData streaming credentials.
4. Stream host and port use BMW defaults automatically (`customer.streaming-cardata.bmwgroup.com:9000`).
5. Keep your BMW `gcid` as MQTT username and ID token as MQTT password (handled automatically by the integration).

When streaming is enabled, telematics sensors use streamed updates while basic vehicle data remains polled.
