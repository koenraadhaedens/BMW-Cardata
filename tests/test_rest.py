"""Isolated REST tests with lightweight Home Assistant interfaces."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import importlib.util
import logging
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch


ROOT = Path(__file__).resolve().parents[1] / "custom_components" / "bmw_cardata"


class Coordinator:
    def __class_getitem__(cls, _item):
        return cls

    def __init__(self, hass, *, logger, **_kwargs):
        self.hass = hass
        self.logger = logger
        self.data = None
        self.last_update_success = True

    def async_set_updated_data(self, data):
        self.data = data


class CoordinatorEntity:
    def __class_getitem__(cls, _item):
        return cls

    def __init__(self, coordinator):
        self.coordinator = coordinator

    @property
    def available(self):
        return self.coordinator.last_update_success


@dataclass(frozen=True, kw_only=True)
class SensorDescription:
    key: str
    translation_key: str | None = None
    entity_category: str | None = None
    native_unit_of_measurement: str | None = None
    device_class: str | None = None
    state_class: str | None = None


class ConfigFlow:
    def __init_subclass__(cls, **_kwargs):
        pass

    def async_show_form(self, **kwargs):
        return kwargs

    def async_abort(self, **kwargs):
        return kwargs

    def async_update_reload_and_abort(self, entry, **kwargs):
        return {"entry": entry, **kwargs}

    async def async_set_unique_id(self, unique_id):
        self.unique_id = unique_id

    def _abort_if_unique_id_configured(self, **_kwargs):
        pass


def load_modules():
    """Load real integration code without starting a Home Assistant instance."""
    interfaces = {
        "homeassistant.config_entries": {
            "ConfigEntry": object, "ConfigFlow": ConfigFlow, "OptionsFlow": object,
        },
        "homeassistant.core": {"HomeAssistant": object, "callback": lambda fn: fn},
        "homeassistant.const": {
            "CONF_CLIENT_ID": "client_id",
            "EntityCategory": SimpleNamespace(DIAGNOSTIC="diagnostic"),
        },
        "homeassistant.helpers.update_coordinator": {
            "DataUpdateCoordinator": Coordinator, "CoordinatorEntity": CoordinatorEntity,
        },
        "homeassistant.helpers.event": {"async_track_time_interval": Mock()},
        "homeassistant.helpers.aiohttp_client": {"async_get_clientsession": Mock()},
        "homeassistant.helpers.entity_platform": {"AddEntitiesCallback": object},
        "homeassistant.components.sensor": {
            "SensorEntity": type("SensorEntity", (), {}),
            "SensorEntityDescription": SensorDescription,
            "SensorDeviceClass": SimpleNamespace(BATTERY="battery"),
            "SensorStateClass": SimpleNamespace(MEASUREMENT="measurement"),
        },
    }
    modules = {}
    for name, values in interfaces.items():
        module = ModuleType(name)
        module.__dict__.update(values)
        modules[name] = module
        parent = name.rpartition(".")[0]
        while parent:
            modules.setdefault(parent, ModuleType(parent))
            parent = parent.rpartition(".")[0]
    for name in ("custom_components", "custom_components.bmw_cardata"):
        module = ModuleType(name)
        module.__path__ = [str(ROOT)]
        modules[name] = module
    for name, module in list(modules.items()):
        parent, _, child = name.rpartition(".")
        if parent in modules:
            setattr(modules[parent], child, module)
    loaded = {}
    with patch.dict(sys.modules, modules):
        for name in (
            "const", "api", "token_manager", "stream_manager", "coordinator",
            "sensor", "config_flow", "integration",
        ):
            full_name = f"custom_components.bmw_cardata.{name}"
            path = ROOT / ("__init__.py" if name == "integration" else f"{name}.py")
            spec = importlib.util.spec_from_file_location(full_name, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[full_name] = module
            spec.loader.exec_module(module)
            loaded[name] = module
    return SimpleNamespace(**loaded)


MODULES = load_modules()
C = MODULES.const
VIN = "WBA00000000000001"


class Clock(datetime):
    current = datetime(2026, 1, 1, tzinfo=UTC)

    @classmethod
    def now(cls, tz=None):
        return cls.current


class RestTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        Clock.current = datetime(2026, 1, 1, tzinfo=UTC)
        self.clock = patch.object(MODULES.coordinator, "datetime", Clock)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.entry = SimpleNamespace(
            entry_id="test",
            options={C.CONF_REST_CONTAINER_ID: "test-container"},
            data={
                C.CONF_SCOPE: f"{C.STREAMING_SCOPE} {C.REST_SCOPE}",
                C.CONF_STREAM_TOPIC: f"vehicles/{VIN}",
            },
            async_start_reauth=Mock(),
            async_on_unload=Mock(),
        )
        self.hass = SimpleNamespace(
            config_entries=SimpleNamespace(
                async_update_entry=Mock(side_effect=self.update_entry),
                async_reload=AsyncMock(),
            ),
            data={},
            async_create_task=asyncio.create_task,
        )
        self.stream = SimpleNamespace(
            enabled=True, async_start=AsyncMock(),
            async_get_telematic_snapshot=AsyncMock(return_value={}),
        )
        self.api = SimpleNamespace(
            async_get_telematic_data=AsyncMock(return_value={
                C.REST_BATTERY_LEVEL: {"value": "97", "timestamp": "stored-time"},
                C.REST_CHARGING_STATUS: {"value": "NOCHARGING"},
                C.REST_CHARGING_METHOD: {"value": "AC"},
                "vehicle.drivetrain.batteryManagement.maxEnergy": {"value": "999"},
            })
        )
        self.tokens = SimpleNamespace(async_get_access_token=AsyncMock(return_value="test"))
        self.coordinator = self.make_coordinator()

    def update_entry(self, entry, *, data):
        entry.data = data

    def make_coordinator(self):
        return MODULES.coordinator.BmwCarDataCoordinator(
            self.hass, entry=self.entry, stream_manager=self.stream,
            rest_api=self.api, token_manager=self.tokens,
        )

    def sensor(self, key):
        description = next(item for item in MODULES.sensor.VEHICLE_SENSORS if item.key == key)
        return MODULES.sensor.BmwCarDataVehicleSensor(
            coordinator=self.coordinator, entry=self.entry, vin=VIN, description=description,
        )

    async def test_twelve_calls_per_day_including_concurrent_refreshes_and_reloads(self):
        for hour in range(24):
            Clock.current = datetime(2026, 1, 1, hour, tzinfo=UTC)
            self.coordinator = self.make_coordinator()
            await asyncio.gather(
                self.coordinator.async_refresh_rest(),
                self.coordinator.async_refresh_rest(),
            )
        self.assertEqual(self.api.async_get_telematic_data.await_count, 12)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 12)

    async def test_only_selected_sensors_use_rest_and_mqtt_does_not_overwrite_them(self):
        await self.coordinator.async_refresh_rest()
        self.stream.async_get_telematic_snapshot.return_value = {VIN: {
            C.REST_BATTERY_LEVEL: {"value": "12"},
            C.REST_CHARGING_STATUS: {"value": "CHARGING"},
            C.REST_CHARGING_METHOD: {"value": "DC"},
            "soc": {"value": "12"},
            "vehicle.drivetrain.batteryManagement.maxEnergy": {"value": "9.58"},
        }}
        await self.coordinator.async_apply_stream_snapshot()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 1)
        self.assertEqual(self.sensor("battery_soc").native_value, 97)
        self.assertEqual(self.sensor("charging_status").native_value, "NOCHARGING")
        self.assertEqual(self.sensor("charging_method").native_value, "AC")
        self.assertEqual(self.sensor("battery_max_energy").native_value, 9.58)
        self.assertEqual(self.sensor("battery_soc").extra_state_attributes["timestamp"], "stored-time")
        self.assertEqual(
            set(self.coordinator.data[C.CONF_REST_DATA_BY_VIN][VIN]), set(C.REST_SENSOR_KEYS)
        )

    async def test_rate_limit_persists_until_utc_day_changes_without_stopping_mqtt(self):
        self.api.async_get_telematic_data.side_effect = MODULES.api.BmwCarDataRateLimitError("quota")
        await self.coordinator.async_refresh_rest()
        self.assertFalse(self.sensor("battery_soc").available)
        self.assertTrue(self.sensor("battery_max_energy").available)
        Clock.current += timedelta(hours=3)
        self.coordinator = self.make_coordinator()
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 1)
        Clock.current = datetime(2026, 1, 2, tzinfo=UTC)
        self.api.async_get_telematic_data.side_effect = None
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 2)
        self.assertTrue(self.sensor("battery_soc").available)

    async def test_failed_reads_count_toward_quota_and_do_not_retry_early(self):
        self.api.async_get_telematic_data.side_effect = MODULES.api.BmwCarDataApiError("offline")
        await self.coordinator.async_refresh_rest()
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 1)
        self.assertEqual(self.api.async_get_telematic_data.await_count, 1)
        self.assertFalse(self.sensor("charging_status").available)
        self.assertTrue(self.sensor("battery_max_energy").available)

    async def test_local_daily_cap_and_missing_scope_make_no_requests(self):
        self.entry.data[C.CONF_REST_REQUEST_STATE] = {
            "date": "2026-01-01", "count": 50, "last_attempt_by_vin": {},
        }
        await self.coordinator.async_refresh_rest()
        self.api.async_get_telematic_data.assert_not_awaited()
        self.entry.data[C.CONF_SCOPE] = C.STREAMING_SCOPE
        await self.coordinator.async_initialize()
        self.entry.async_start_reauth.assert_called_once_with(self.hass)
        self.stream.async_start.assert_awaited_once()
        self.api.async_get_telematic_data.assert_not_awaited()

    async def test_multiple_vehicles_cannot_exceed_fifty_daily_calls(self):
        self.coordinator._telematic_cache_by_vin = {
            f"WBA0000000000000{i}": {} for i in range(1, 6)
        }
        for hour in range(24):
            Clock.current = datetime(2026, 1, 1, hour, tzinfo=UTC)
            await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 50)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 50)
        Clock.current = datetime(2026, 1, 2, tzinfo=UTC)
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 55)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 5)

    async def test_authorization_failure_recovers_after_reauth_without_daily_block(self):
        self.api.async_get_telematic_data.side_effect = MODULES.api.BmwCarDataRestAuthError("denied")
        await self.coordinator.async_refresh_rest()
        self.entry.async_start_reauth.assert_called_once_with(self.hass)
        Clock.current += timedelta(hours=2)
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 1)
        self.coordinator = self.make_coordinator()
        self.api.async_get_telematic_data.side_effect = None
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 2)
        self.assertTrue(self.sensor("charging_status").available)

    async def test_rest_waits_for_vin_discovery_without_listing_vehicles(self):
        self.entry.data[C.CONF_STREAM_TOPIC] = "vehicles/#"
        await self.coordinator.async_initialize()
        self.api.async_get_telematic_data.assert_not_awaited()
        self.stream.async_get_telematic_snapshot.return_value = {VIN: {"soc": {"value": "40"}}}
        tasks = []
        self.hass.async_create_task = lambda coro: tasks.append(asyncio.create_task(coro))
        await self.coordinator.async_apply_stream_snapshot()
        await asyncio.gather(*tasks)
        self.api.async_get_telematic_data.assert_awaited_once()
        self.assertEqual(self.sensor("battery_soc").native_value, 97)

    async def test_rest_disabled_keeps_existing_mqtt_behavior(self):
        self.entry.options[C.CONF_REST_CONTAINER_ID] = ""
        self.stream.async_get_telematic_snapshot.return_value = {
            VIN: {"soc": {"value": "42"}, C.REST_CHARGING_STATUS: {"value": "CHARGING"}}
        }
        await self.coordinator.async_initialize()
        await self.coordinator.async_apply_stream_snapshot()
        self.assertEqual(self.sensor("battery_soc").native_value, 42)
        self.assertEqual(self.sensor("charging_status").native_value, "CHARGING")
        self.api.async_get_telematic_data.assert_not_awaited()

    async def test_missing_and_null_rest_fields_never_fall_back_to_mqtt(self):
        self.api.async_get_telematic_data.return_value = {C.REST_BATTERY_LEVEL: {"value": None}}
        await self.coordinator.async_refresh_rest()
        self.coordinator._telematic_cache_by_vin[VIN] = {
            "soc": {"value": "70"}, C.REST_CHARGING_STATUS: {"value": "CHARGING"},
        }
        self.coordinator.async_set_updated_data(self.coordinator._build_payload())
        self.assertIsNone(self.sensor("battery_soc").native_value)
        self.assertIsNone(self.sensor("charging_status").native_value)

    async def test_data_updates_do_not_reload_options_updates_do(self):
        self.hass.data = {C.DOMAIN: {C.DATA_ENTRIES: {
            self.entry.entry_id: {"options": dict(self.entry.options)}
        }}}
        await MODULES.integration._async_options_updated(self.hass, self.entry)
        self.hass.config_entries.async_reload.assert_not_awaited()
        self.entry.options = {C.CONF_REST_CONTAINER_ID: "different-container"}
        await MODULES.integration._async_options_updated(self.hass, self.entry)
        self.hass.config_entries.async_reload.assert_awaited_once_with(self.entry.entry_id)

    async def test_mqtt_and_rest_share_a_single_token_refresh(self):
        self.entry.data.update(
            access_token="old", id_token="old-id", refresh_token="old-refresh",
            token_expires_at="2025-12-31T00:00:00+00:00", client_id="test-client",
        )

        async def refresh(**_kwargs):
            await asyncio.sleep(0)
            return MODULES.api.TokenResponse(
                access_token="new", id_token="new-id", refresh_token="new-refresh",
                token_type="Bearer", expires_in=3600, scope=None, gcid=None,
            )

        auth = SimpleNamespace(refresh_token=AsyncMock(side_effect=refresh))
        manager = MODULES.token_manager.BmwCarDataTokenManager(self.hass, self.entry, auth)
        values = await asyncio.gather(manager.async_get_access_token(), manager.async_get_id_token())
        self.assertEqual(values, ["new", "new-id"])
        auth.refresh_token.assert_awaited_once()


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_headers_and_invalid_response_and_http_errors(self):
        for status, body, expected_error in (
            (200, {"telematicData": {C.REST_BATTERY_LEVEL: {"value": "97"}}}, None),
            (200, {"telematicData": []}, MODULES.api.BmwCarDataApiError),
            (401, {}, MODULES.api.BmwCarDataRestAuthError),
            (403, {}, MODULES.api.BmwCarDataRestAuthError),
            (429, {}, MODULES.api.BmwCarDataRateLimitError),
            (500, {}, MODULES.api.BmwCarDataApiError),
        ):
            with self.subTest(status=status, body=body):
                response = SimpleNamespace(status=status, json=AsyncMock(return_value=body))
                context = Mock(
                    __aenter__=AsyncMock(return_value=response),
                    __aexit__=AsyncMock(return_value=False),
                )
                session = SimpleNamespace(get=Mock(return_value=context))
                api = MODULES.api.BmwCarDataRestApi(session)
                call = api.async_get_telematic_data(
                    access_token="test-token", vin=VIN, container_id="container & test",
                )
                if expected_error:
                    with self.assertRaises(expected_error):
                        await call
                else:
                    self.assertEqual(await call, body["telematicData"])
                    args, kwargs = session.get.call_args
                    self.assertIn("containerId=container+%26+test", args[0])
                    self.assertEqual(kwargs["headers"]["x-version"], "v1")
                    self.assertEqual(kwargs["headers"]["Authorization"], "Bearer test-token")
                if status >= 400:
                    response.json.assert_not_awaited()

    async def test_setup_requests_rest_scope_only_when_enabled(self):
        for container_id in ("", "container"):
            with self.subTest(container_id=container_id):
                flow = MODULES.config_flow.BmwCarDataConfigFlow()
                flow.hass = SimpleNamespace()
                auth = Mock()
                auth.generate_pkce_pair.return_value = SimpleNamespace(
                    code_challenge="challenge", code_verifier="verifier",
                )
                auth.request_device_code = AsyncMock(return_value=SimpleNamespace(
                    device_code="device", user_code="user", verification_uri="https://bmw.example",
                    expires_in=300,
                ))
                with patch.object(MODULES.config_flow, "BmwCarDataAuthApi", return_value=auth):
                    await flow.async_step_user({
                        "client_id": "test", "stream_topic": VIN,
                        C.CONF_REST_CONTAINER_ID: container_id,
                    })
                scopes = auth.request_device_code.call_args.kwargs["scope"].split()
                self.assertIn(C.STREAMING_SCOPE, scopes)
                self.assertEqual(C.REST_SCOPE in scopes, bool(container_id))

    async def test_reauthorization_preserves_entry_and_checks_account_and_scope(self):
        for gcid, scope, expected in (
            ("account", C.REST_SCOPE, "success"),
            ("different-account", C.REST_SCOPE, "wrong_account"),
            ("account", C.STREAMING_SCOPE, "rest_scope_missing"),
        ):
            with self.subTest(gcid=gcid, scope=scope):
                flow = MODULES.config_flow.BmwCarDataConfigFlow()
                flow.hass = SimpleNamespace()
                flow._reauth_entry = SimpleNamespace(unique_id="account")
                flow._client_id = "client"
                flow._device_code = "device"
                flow._code_verifier = "verifier"
                flow._stream_options = {C.CONF_REST_CONTAINER_ID: "container"}
                auth = SimpleNamespace(request_token_with_device_code=AsyncMock(
                    return_value=MODULES.api.TokenResponse(
                        access_token="new", token_type="Bearer", expires_in=3600,
                        refresh_token="new-refresh", scope=scope, gcid=gcid, id_token="id",
                    )
                ))
                with patch.object(MODULES.config_flow, "BmwCarDataAuthApi", return_value=auth):
                    result = await flow.async_step_authorize({})
                if expected == "success":
                    self.assertIs(result["entry"], flow._reauth_entry)
                    self.assertEqual(result["data_updates"][C.CONF_ACCESS_TOKEN], "new")
                elif expected == "wrong_account":
                    self.assertEqual(result["reason"], expected)
                else:
                    self.assertEqual(result["errors"]["base"], expected)


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    unittest.main()
