"""Isolated integration tests with lightweight Home Assistant interfaces."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import importlib.util
import json
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


class HomeAssistantError(Exception):
    def __init__(self, *, translation_domain, translation_key, translation_placeholders):
        super().__init__(translation_placeholders["reason"])
        self.translation_domain = translation_domain
        self.translation_key = translation_key


@dataclass(frozen=True, kw_only=True)
class SensorDescription:
    key: str
    icon: str | None = None
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

    def async_create_entry(self, **kwargs):
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
            "ConfigEntry": object, "ConfigFlow": ConfigFlow, "OptionsFlow": ConfigFlow,
        },
        "homeassistant.core": {"HomeAssistant": object, "callback": lambda fn: fn},
        "homeassistant.exceptions": {"HomeAssistantError": HomeAssistantError},
        "homeassistant.const": {
            "CONF_CLIENT_ID": "client_id",
            "EntityCategory": SimpleNamespace(DIAGNOSTIC="diagnostic", CONFIG="config"),
        },
        "homeassistant.helpers.update_coordinator": {
            "DataUpdateCoordinator": Coordinator, "CoordinatorEntity": CoordinatorEntity,
        },
        "homeassistant.helpers.event": {"async_track_time_interval": Mock()},
        "homeassistant.helpers.importlib": {
            "async_import_module": AsyncMock(side_effect=lambda _hass, name: sys.modules[name]),
        },
        "homeassistant.helpers.aiohttp_client": {"async_get_clientsession": Mock()},
        "homeassistant.helpers.entity_platform": {"AddEntitiesCallback": object},
        "homeassistant.helpers.device_registry": {"DeviceInfo": dict},
        "homeassistant.components.sensor": {
            "SensorEntity": type("SensorEntity", (), {}),
            "SensorEntityDescription": SensorDescription,
            "SensorDeviceClass": SimpleNamespace(
                BATTERY="battery", DISTANCE="distance", VOLUME_STORAGE="volume_storage",
                ENERGY_STORAGE="energy_storage",
            ),
            "SensorStateClass": SimpleNamespace(
                MEASUREMENT="measurement", TOTAL_INCREASING="total_increasing",
            ),
        },
        "homeassistant.components.binary_sensor": {
            "BinarySensorEntity": type("BinarySensorEntity", (), {}),
            "BinarySensorEntityDescription": SensorDescription,
            "BinarySensorDeviceClass": SimpleNamespace(BATTERY_CHARGING="battery_charging"),
        },
        "homeassistant.components.button": {"ButtonEntity": type("ButtonEntity", (), {})},
        "homeassistant.components.device_tracker": {
            "TrackerEntity": type("TrackerEntity", (), {}),
            "SourceType": SimpleNamespace(GPS="gps"),
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
            "entity", "sensor", "binary_sensor", "button", "device_tracker",
            "config_flow", "integration",
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


class VehicleEntityTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_platforms_group_existing_and_new_vehicles_without_duplicates(self):
        entry = SimpleNamespace(entry_id="test", async_on_unload=Mock())
        coordinator = SimpleNamespace(
            data={C.CONF_MAPPINGS: [{"vin": VIN}]},
            use_rest=True,
            async_add_listener=Mock(return_value=lambda: None),
        )
        hass = SimpleNamespace(data={
            C.DOMAIN: {C.DATA_ENTRIES: {"test": {"coordinator": coordinator}}},
        })
        second_vin = "WBA00000000000002"
        for platform in (
            MODULES.sensor, MODULES.binary_sensor, MODULES.device_tracker, MODULES.button,
        ):
            with self.subTest(platform=platform.__name__):
                coordinator.data[C.CONF_MAPPINGS] = [{"vin": VIN}]
                entities = []
                await platform.async_setup_entry(hass, entry, entities.extend)
                listener = coordinator.async_add_listener.call_args.args[0]
                original_count = len(entities)
                coordinator.data[C.CONF_MAPPINGS].append({"vin": second_vin})
                listener()
                listener()
                vehicle_entities = [entity for entity in entities if hasattr(entity, "_vin")]
                first = [entity for entity in vehicle_entities if entity._vin == VIN]
                second = [entity for entity in vehicle_entities if entity._vin == second_vin]
                self.assertTrue(first)
                self.assertEqual(len(first), len(second))
                self.assertEqual(len(entities), original_count + len(second))
                for entity in vehicle_entities:
                    self.assertEqual(entity._attr_device_info, {
                        "identifiers": {(C.DOMAIN, entity._vin)},
                        "name": f"BMW {entity._vin}",
                        "manufacturer": "BMW",
                    })
                    key = (
                        "tracker" if platform is MODULES.device_tracker
                        else "refresh_rest" if platform is MODULES.button
                        else entity.entity_description.key
                    )
                    self.assertEqual(entity._attr_unique_id, f"test_{entity._vin}_{key}")
                    self.assertFalse(hasattr(entity, "_attr_name"))
                if platform is MODULES.sensor:
                    mapped = next(entity for entity in entities if not hasattr(entity, "_vin"))
                    self.assertFalse(hasattr(mapped, "_attr_device_info"))

    def test_vehicle_labels_are_translated_and_daily_readings_are_not_diagnostics(self):
        strings = json.loads((ROOT / "strings.json").read_text(encoding="utf-8"))
        translations = json.loads(
            (ROOT / "translations" / "en.json").read_text(encoding="utf-8")
        )
        self.assertEqual(strings["entity"], translations["entity"])
        for platform, descriptions in (
            ("sensor", MODULES.sensor.VEHICLE_SENSORS),
            ("binary_sensor", MODULES.binary_sensor.VEHICLE_BINARY_SENSORS),
        ):
            for description in descriptions:
                with self.subTest(platform=platform, key=description.key):
                    self.assertTrue(strings["entity"][platform][description.translation_key]["name"])
                    if description.key not in ("current_latitude", "current_longitude"):
                        self.assertIsNone(description.entity_category)
                    self.assertTrue(description.icon or description.device_class)
        self.assertTrue(strings["entity"]["device_tracker"]["location"]["name"])
        self.assertTrue(strings["entity"]["button"]["refresh_rest"]["name"])


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
            version=2,
            options={C.CONF_USE_REST: True},
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
            async_list_containers=AsyncMock(return_value=[{
                "containerId": "test-container", "state": "ACTIVE",
                "created": "2025-12-01T00:00:00Z",
            }]),
            async_get_container_descriptors=AsyncMock(return_value=list(C.REST_SENSOR_KEYS)),
            async_create_container=AsyncMock(return_value="created-container"),
            async_get_telematic_data=AsyncMock(return_value={
                C.REST_BATTERY_LEVEL: {"value": "97", "timestamp": "stored-time"},
                C.REST_CHARGING_STATUS: {"value": "NOCHARGING"},
                C.REST_CHARGING_METHOD: {"value": "AC"},
                "vehicle.drivetrain.batteryManagement.maxEnergy": {"value": "999"},
            })
        )
        self.tokens = SimpleNamespace(async_get_access_token=AsyncMock(return_value="test"))
        self.coordinator = self.make_coordinator()

    def update_entry(self, entry, **kwargs):
        for key, value in kwargs.items():
            setattr(entry, key, value)

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
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 14)
        self.api.async_list_containers.assert_awaited_once()
        self.api.async_get_container_descriptors.assert_awaited_once()

    async def test_rest_cache_restores_during_cooldown_without_new_api_calls(self):
        await self.coordinator.async_refresh_rest()
        self.entry.data[C.CONF_STREAM_TOPIC] = "#"
        self.api.async_get_telematic_data.reset_mock()
        self.api.async_list_containers.reset_mock()
        self.api.async_get_container_descriptors.reset_mock()
        for minutes in (10, 20):
            Clock.current = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=minutes)
            self.coordinator = self.make_coordinator()
            await self.coordinator.async_initialize()
            self.assertEqual(
                [mapping["vin"] for mapping in self.coordinator.data[C.CONF_MAPPINGS]], [VIN]
            )
            self.assertTrue(self.sensor("battery_soc").available)
            self.assertEqual(self.sensor("battery_soc").native_value, 97)
            self.assertEqual(self.sensor("charging_status").native_value, "NOCHARGING")
            self.assertEqual(self.sensor("charging_method").native_value, "AC")
            self.assertEqual(self.sensor("battery_soc").extra_state_attributes["timestamp"], "stored-time")
        self.api.async_get_telematic_data.assert_not_awaited()
        self.api.async_list_containers.assert_not_awaited()
        self.api.async_get_container_descriptors.assert_not_awaited()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 3)

    async def test_rest_cache_preserves_failure_unavailability_across_reload(self):
        await self.coordinator.async_refresh_rest()
        Clock.current += timedelta(hours=2)
        self.api.async_get_telematic_data.side_effect = MODULES.api.BmwCarDataApiError("offline")
        await self.coordinator.async_refresh_rest()
        self.api.async_get_telematic_data.reset_mock()
        Clock.current += timedelta(minutes=10)
        self.coordinator = self.make_coordinator()
        await self.coordinator.async_initialize()
        self.assertFalse(self.sensor("battery_soc").available)
        self.assertFalse(self.sensor("charging_status").available)
        self.assertTrue(self.sensor("odometer").available)
        self.api.async_get_telematic_data.assert_not_awaited()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 4)

    async def test_rest_cache_does_not_hide_a_persisted_quota_failure(self):
        await self.coordinator.async_refresh_rest()
        self.entry.data[C.CONF_REST_REQUEST_STATE]["count"] = 50
        Clock.current += timedelta(hours=2)
        await self.coordinator.async_refresh_rest()
        self.api.async_get_telematic_data.reset_mock()
        self.coordinator = self.make_coordinator()
        await self.coordinator.async_initialize()
        self.assertFalse(self.sensor("battery_soc").available)
        self.assertTrue(self.entry.data[C.CONF_REST_REQUEST_STATE]["blocked"])
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 50)
        self.api.async_get_telematic_data.assert_not_awaited()

    async def test_rest_cache_absent_on_upgrade_does_not_bypass_the_cooldown(self):
        self.entry.data[C.CONF_REST_REQUEST_STATE] = {
            "date": "2026-01-01", "count": 1,
            "last_attempt_by_vin": {VIN: Clock.current.isoformat()},
            "container_id": "test-container", "container_checked_date": "2026-01-01",
        }
        self.coordinator = self.make_coordinator()
        Clock.current += timedelta(minutes=30)
        with self.assertLogs(MODULES.coordinator.__name__, level="DEBUG") as logs:
            await self.coordinator.async_initialize()
        output = "\n".join(logs.output)
        self.assertIn("eligible at 2026-01-01T02:00:00+00:00", output)
        self.assertIn("saved fields=0, available=False", output)
        self.assertIn("next periodic check at approximately 2026-01-01T02:30:00+00:00", output)
        self.assertFalse(self.sensor("battery_soc").available)
        self.api.async_get_telematic_data.assert_not_awaited()
        Clock.current += timedelta(hours=1, minutes=30)
        with self.assertLogs(MODULES.coordinator.__name__, level="DEBUG") as logs:
            await self.coordinator.async_refresh_rest()
        output = "\n".join(logs.output)
        self.assertIn("local calls=2/50", output)
        self.assertIn("selected fields=3/3", output)
        self.assertNotIn("test-container", output)
        self.assertNotIn("NOCHARGING", output)
        self.assertTrue(self.sensor("battery_soc").available)
        self.assertEqual(self.entry.data[C.CONF_REST_DATA_BY_VIN][VIN][C.REST_BATTERY_LEVEL]["value"], "97")
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 2)

    async def test_rest_diagnostics_distinguish_discovery_and_persisted_quota_pause(self):
        self.entry.data[C.CONF_STREAM_TOPIC] = "#"
        with self.assertLogs(MODULES.coordinator.__name__, level="DEBUG") as logs:
            await self.coordinator.async_initialize()
        self.assertIn("waiting for MQTT vehicle discovery", "\n".join(logs.output))
        self.entry.data[C.CONF_REST_REQUEST_STATE] = {
            "date": "2026-01-01", "count": 50, "blocked": True,
            "last_attempt_by_vin": {},
        }
        self.entry.data[C.CONF_STREAM_TOPIC] = VIN
        with self.assertLogs(MODULES.coordinator.__name__, level="DEBUG") as logs:
            await self.coordinator.async_refresh_rest()
        self.assertIn("daily quota blocked for 2026-01-01", "\n".join(logs.output))
        self.tokens.async_get_access_token.assert_not_awaited()
        self.api.async_get_telematic_data.assert_not_awaited()
        self.api.async_list_containers.assert_not_awaited()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 50)

    async def test_manual_rest_refresh_bypasses_only_target_cooldown_and_resets_it(self):
        other_vin = "WBA00000000000002"
        self.coordinator._telematic_cache_by_vin[other_vin] = {}
        await self.coordinator.async_refresh_rest()
        self.api.async_get_telematic_data.reset_mock()
        Clock.current += timedelta(minutes=30)
        self.api.async_get_telematic_data.return_value[C.REST_BATTERY_LEVEL]["value"] = "80"
        await self.coordinator.async_manual_refresh_rest(VIN)
        self.api.async_get_telematic_data.assert_awaited_once_with(
            access_token="test", vin=VIN, container_id="test-container"
        )
        self.assertEqual(self.sensor("battery_soc").native_value, 80)
        state = self.entry.data[C.CONF_REST_REQUEST_STATE]
        self.assertEqual(state["count"], 5)
        self.assertEqual(state["last_attempt_by_vin"][VIN], Clock.current.isoformat())
        self.assertEqual(state["last_attempt_by_vin"][other_vin], "2026-01-01T00:00:00+00:00")
        self.coordinator = self.make_coordinator()
        self.api.async_get_telematic_data.reset_mock()
        Clock.current += timedelta(hours=1, minutes=30)
        await self.coordinator.async_refresh_rest()
        self.api.async_get_telematic_data.assert_awaited_once_with(
            access_token="test", vin=other_vin, container_id="test-container"
        )
        Clock.current += timedelta(minutes=30)
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 2)
        self.assertEqual(self.api.async_get_telematic_data.await_args.kwargs["vin"], VIN)

    async def test_manual_rest_refresh_enforces_local_cap_and_http_quota_block(self):
        await self.coordinator.async_refresh_rest()
        self.entry.data[C.CONF_REST_REQUEST_STATE]["count"] = 49
        await self.coordinator.async_manual_refresh_rest(VIN)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 50)
        with self.assertRaises(HomeAssistantError) as error:
            await self.coordinator.async_manual_refresh_rest(VIN)
        self.assertEqual(error.exception.translation_key, "rest_refresh_failed")
        self.assertEqual(self.api.async_get_telematic_data.await_count, 2)
        self.assertFalse(self.sensor("battery_soc").available)
        self.assertTrue(self.entry.data[C.CONF_REST_REQUEST_STATE]["blocked"])
        with self.assertRaises(HomeAssistantError):
            await self.coordinator.async_manual_refresh_rest(VIN)
        self.assertEqual(self.api.async_get_telematic_data.await_count, 2)
        Clock.current += timedelta(days=1)
        self.api.async_get_telematic_data.side_effect = MODULES.api.BmwCarDataRateLimitError(
            "BMW daily REST quota exhausted (HTTP 429)"
        )
        with self.assertRaisesRegex(HomeAssistantError, "429"):
            await self.coordinator.async_manual_refresh_rest(VIN)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 3)
        self.assertTrue(self.entry.data[C.CONF_REST_REQUEST_STATE]["blocked"])
        with self.assertRaises(HomeAssistantError):
            await self.coordinator.async_manual_refresh_rest(VIN)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 3)

    async def test_manual_rest_refresh_surfaces_failure_and_preserves_automatic_cooldown(self):
        await self.coordinator.async_refresh_rest()
        Clock.current += timedelta(minutes=30)
        self.api.async_get_telematic_data.side_effect = MODULES.api.BmwCarDataApiError("offline")
        button = MODULES.button.BmwCarDataRestRefreshButton(
            coordinator=self.coordinator, entry=self.entry, vin=VIN
        )
        with self.assertRaisesRegex(HomeAssistantError, "offline"):
            await button.async_press()
        self.assertFalse(self.sensor("battery_soc").available)
        self.assertTrue(button.available)
        self.coordinator = self.make_coordinator()
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 2)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 4)
        self.assertFalse(self.sensor("battery_soc").available)

    async def test_manual_rest_refresh_rejects_disabled_scope_and_unknown_vehicle(self):
        for case, reason in (
            ("disabled", "disabled"), ("scope", "authorization"), ("unknown", "discovered"),
        ):
            with self.subTest(case=case):
                self.entry.options[C.CONF_USE_REST] = case != "disabled"
                self.entry.data[C.CONF_SCOPE] = (
                    C.STREAMING_SCOPE if case == "scope" else f"{C.STREAMING_SCOPE} {C.REST_SCOPE}"
                )
                with self.assertRaisesRegex(HomeAssistantError, reason):
                    await self.coordinator.async_manual_refresh_rest("WBA00000000000002")
        self.api.async_list_containers.assert_not_awaited()
        self.api.async_get_telematic_data.assert_not_awaited()

    async def test_manual_rest_button_discovery_deduplicates_and_skips_mqtt_only(self):
        self.entry.data[C.CONF_STREAM_TOPIC] = "#"
        self.hass.data[C.DOMAIN] = {C.DATA_ENTRIES: {
            self.entry.entry_id: {"coordinator": self.coordinator},
        }}
        self.coordinator.async_add_listener = Mock()
        self.coordinator.async_set_updated_data({C.CONF_MAPPINGS: []})
        add_entities = Mock()
        await MODULES.button.async_setup_entry(self.hass, self.entry, add_entities)
        add_entities.assert_not_called()
        listener = self.coordinator.async_add_listener.call_args.args[0]
        self.coordinator._telematic_cache_by_vin[VIN] = {}
        self.coordinator.async_set_updated_data(self.coordinator._build_payload())
        listener()
        listener()
        add_entities.assert_called_once()
        button = add_entities.call_args.args[0][0]
        self.assertTrue(button.available)
        await button.async_press()
        self.assertEqual(self.sensor("battery_soc").native_value, 97)
        self.api.async_get_telematic_data.assert_awaited_once()
        self.entry.options[C.CONF_USE_REST] = False
        self.assertFalse(button.available)
        add_entities.reset_mock()
        await MODULES.button.async_setup_entry(self.hass, self.entry, add_entities)
        add_entities.assert_not_called()

    async def test_manual_rest_refresh_surfaces_container_and_authorization_failures(self):
        self.api.async_list_containers.side_effect = MODULES.api.BmwCarDataApiError(
            "container discovery offline"
        )
        with self.assertRaisesRegex(HomeAssistantError, "container discovery offline"):
            await self.coordinator.async_manual_refresh_rest(VIN)
        self.api.async_get_telematic_data.assert_not_awaited()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 1)
        self.api.async_list_containers.side_effect = None
        Clock.current += timedelta(hours=2)
        self.api.async_get_telematic_data.side_effect = MODULES.api.BmwCarDataRestAuthError(
            "BMW authorization expired"
        )
        with self.assertRaisesRegex(HomeAssistantError, "authorization expired"):
            await self.coordinator.async_manual_refresh_rest(VIN)
        self.entry.async_start_reauth.assert_called_once_with(self.hass)
        self.assertFalse(self.sensor("battery_soc").available)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 4)
        with self.assertRaisesRegex(HomeAssistantError, "reauthorization"):
            await self.coordinator.async_manual_refresh_rest(VIN)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 4)

    async def test_rest_cache_rejects_invalid_saved_entries_with_an_error(self):
        self.entry.data[C.CONF_REST_DATA_BY_VIN] = {VIN: {C.REST_BATTERY_LEVEL: "invalid"}}
        self.entry.data[C.CONF_REST_AVAILABLE_BY_VIN] = {VIN: True}
        self.entry.data[C.CONF_STREAM_TOPIC] = "#"
        with self.assertLogs(MODULES.coordinator.__name__, level="ERROR"):
            self.coordinator = self.make_coordinator()
        await self.coordinator.async_initialize()
        self.assertFalse(self.sensor("battery_soc").available)
        self.assertEqual(self.coordinator.data[C.CONF_MAPPINGS], [])
        self.api.async_get_telematic_data.assert_not_awaited()

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

    async def test_total_remaining_range_uses_combined_mqtt_value_not_other_ranges(self):
        self.stream.async_get_telematic_snapshot.return_value = {VIN: {
            "vehicle.drivetrain.lastRemainingRange": {"value": "520.5", "unit": "km"},
            "vehicle.drivetrain.totalRemainingRange": {"value": "600"},
            "vehicle.drivetrain.electricEngine.kombiRemainingElectricRange": {"value": "33"},
        }}
        await self.coordinator.async_apply_stream_snapshot()
        self.assertEqual(self.sensor("total_remaining_range").native_value, 520.5)
        self.assertEqual(self.sensor("electric_remaining_range").native_value, 33)
        self.api.async_get_telematic_data.assert_not_awaited()
        self.api.async_list_containers.assert_not_awaited()

    async def test_total_remaining_range_is_unknown_when_combined_mqtt_field_is_missing(self):
        self.coordinator._rest_data_by_vin[VIN] = {
            "vehicle.drivetrain.lastRemainingRange": {"value": "999"},
        }
        self.stream.async_get_telematic_snapshot.return_value = {VIN: {
            "vehicle.drivetrain.totalRemainingRange": {"value": "600"},
            "vehicle.drivetrain.electricEngine.kombiRemainingElectricRange": {"value": "33"},
        }}
        await self.coordinator.async_apply_stream_snapshot()
        self.assertIsNone(self.sensor("total_remaining_range").native_value)

    async def test_total_remaining_range_tracks_updates_zero_and_unavailable_values(self):
        for value, expected in (("100", 100), ("0", 0), (None, None), ("INVALID", None)):
            with self.subTest(value=value):
                self.stream.async_get_telematic_snapshot.return_value = {VIN: {
                    "vehicle.drivetrain.lastRemainingRange": {"value": value},
                }}
                await self.coordinator.async_apply_stream_snapshot()
                self.assertEqual(self.sensor("total_remaining_range").native_value, expected)
        self.api.async_get_telematic_data.assert_not_awaited()

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
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 3)
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
        self.assertEqual(self.api.async_get_telematic_data.await_count, 48)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 50)
        Clock.current = datetime(2026, 1, 2, tzinfo=UTC)
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_get_telematic_data.await_count, 53)
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 7)

    async def test_newest_compatible_container_wins_without_creation(self):
        self.api.async_list_containers.return_value = [
            {"containerId": "old", "created": "2025-10-01T00:00:00Z"},
            {"containerId": "unrelated", "created": "2025-12-01T00:00:00Z"},
            {"containerId": "compatible", "created": "2025-11-01T00:00:00Z"},
        ]
        self.api.async_get_container_descriptors.side_effect = (
            lambda *, access_token, container_id:
            [] if container_id == "unrelated" else list(C.REST_SENSOR_KEYS)
        )
        await self.coordinator.async_refresh_rest()
        self.assertEqual(
            [call.kwargs["container_id"] for call in self.api.async_get_container_descriptors.call_args_list],
            ["unrelated", "compatible"],
        )
        self.assertEqual(
            self.api.async_get_telematic_data.call_args.kwargs["container_id"], "compatible"
        )
        self.api.async_create_container.assert_not_awaited()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 4)

    async def test_empty_or_incompatible_containers_create_once_and_cache_across_reload(self):
        for empty in (True, False):
            with self.subTest(empty=empty):
                self.entry.data.pop(C.CONF_REST_REQUEST_STATE, None)
                self.coordinator = self.make_coordinator()
                self.api.async_list_containers.reset_mock()
                self.api.async_create_container.reset_mock()
                self.api.async_list_containers.return_value = (
                    [] if empty else [{"containerId": "unrelated", "created": "2025-12-01T00:00:00Z"}]
                )
                self.api.async_get_container_descriptors.return_value = []
                await self.coordinator.async_refresh_rest()
                self.assertEqual(
                    self.api.async_get_telematic_data.call_args.kwargs["container_id"],
                    "created-container",
                )
                Clock.current += timedelta(hours=2)
                self.coordinator = self.make_coordinator()
                await self.coordinator.async_refresh_rest()
                self.api.async_list_containers.assert_awaited_once()
                self.api.async_create_container.assert_awaited_once()

    async def test_container_selection_refreshes_on_new_utc_day(self):
        await self.coordinator.async_refresh_rest()
        self.api.async_list_containers.return_value = [
            {"containerId": "newest", "created": "2025-12-02T00:00:00Z"}
        ]
        Clock.current = datetime(2026, 1, 2, tzinfo=UTC)
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_list_containers.await_count, 2)
        self.assertEqual(
            self.api.async_get_telematic_data.call_args.kwargs["container_id"], "newest"
        )

    async def test_failed_discovery_never_creates_blindly_and_retries_after_two_hours(self):
        self.api.async_list_containers.side_effect = MODULES.api.BmwCarDataApiError("offline")
        await self.coordinator.async_refresh_rest()
        self.coordinator = self.make_coordinator()
        await self.coordinator.async_refresh_rest()
        self.api.async_list_containers.assert_awaited_once()
        self.api.async_create_container.assert_not_awaited()
        self.api.async_get_telematic_data.assert_not_awaited()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 1)
        Clock.current += timedelta(hours=2)
        self.api.async_list_containers.side_effect = None
        await self.coordinator.async_refresh_rest()
        self.assertEqual(self.api.async_list_containers.await_count, 2)
        self.assertTrue(self.sensor("battery_soc").available)

    async def test_discovery_cannot_exceed_budget_or_create_after_a_rate_limit(self):
        self.entry.data[C.CONF_REST_REQUEST_STATE] = {
            "date": "2026-01-01", "count": 49, "last_attempt_by_vin": {},
        }
        self.api.async_list_containers.return_value = []
        await self.coordinator.async_refresh_rest()
        self.api.async_list_containers.assert_awaited_once()
        self.api.async_create_container.assert_not_awaited()
        self.api.async_get_telematic_data.assert_not_awaited()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 50)
        self.assertTrue(self.entry.data[C.CONF_REST_REQUEST_STATE]["blocked"])

    async def test_ambiguous_creation_failure_rediscovers_before_creating_again(self):
        self.api.async_list_containers.return_value = []
        self.api.async_create_container.side_effect = MODULES.api.BmwCarDataApiError("timeout")
        await self.coordinator.async_refresh_rest()
        self.coordinator = self.make_coordinator()
        await self.coordinator.async_refresh_rest()
        self.api.async_create_container.assert_awaited_once()
        self.api.async_list_containers.assert_awaited_once()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 2)
        Clock.current += timedelta(hours=2)
        self.api.async_list_containers.return_value = [
            {"containerId": "created-despite-timeout", "created": "2025-12-01T00:00:00Z"}
        ]
        await self.coordinator.async_refresh_rest()
        self.api.async_create_container.assert_awaited_once()
        self.assertEqual(
            self.api.async_get_telematic_data.call_args.kwargs["container_id"],
            "created-despite-timeout",
        )

    async def test_full_incompatible_account_does_not_create_or_repeat_checks_same_day(self):
        self.api.async_list_containers.return_value = [
            {"containerId": str(i), "created": "2025-12-01T00:00:00Z"}
            for i in range(10)
        ]
        self.api.async_get_container_descriptors.return_value = []
        await self.coordinator.async_refresh_rest()
        Clock.current += timedelta(hours=2)
        self.coordinator = self.make_coordinator()
        await self.coordinator.async_refresh_rest()
        self.api.async_list_containers.assert_awaited_once()
        self.assertEqual(self.api.async_get_container_descriptors.await_count, 10)
        self.api.async_create_container.assert_not_awaited()
        self.api.async_get_telematic_data.assert_not_awaited()
        self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE]["count"], 11)

    async def test_wildcard_default_maps_two_vehicles_from_mqtt(self):
        self.entry.data.pop(C.CONF_STREAM_TOPIC)
        self.entry.data[C.CONF_GCID] = "account"
        manager = MODULES.stream_manager.BmwCarDataStreamManager(
            self.hass, entry=self.entry, token_manager=self.tokens,
        )
        self.assertTrue(manager.enabled)
        topic = manager._config_value(C.CONF_STREAM_TOPIC)
        self.assertEqual(manager._build_subscription_topic("account", topic), "account/#")
        second_vin = "WBA00000000000002"
        updates = {}
        for vin in (VIN, second_vin):
            updates.update(manager._extract_telematic_updates(
                f"account/{vin}", {"name": "soc", "value": "40"}
            ))
        self.stream.async_get_telematic_snapshot.return_value = updates
        tasks = []
        self.hass.async_create_task = lambda coro: tasks.append(asyncio.create_task(coro))
        await self.coordinator.async_apply_stream_snapshot()
        await asyncio.gather(*tasks)
        self.assertEqual(len(self.coordinator.data[C.CONF_MAPPINGS]), 2)
        self.assertEqual(self.api.async_get_telematic_data.await_count, 2)
        self.api.async_list_containers.assert_awaited_once()

    async def test_legacy_migration_preserves_rest_choice_topic_and_quota(self):
        for legacy_id, expected in (("legacy-container", True), ("", False)):
            with self.subTest(legacy_id=legacy_id):
                self.entry.version = 1
                self.entry.options = {
                    C.CONF_REST_CONTAINER_ID: legacy_id, C.CONF_STREAM_TOPIC: "#",
                }
                quota = {"date": "2026-01-01", "count": 20, "last_attempt_by_vin": {}}
                self.entry.data[C.CONF_REST_REQUEST_STATE] = quota
                self.entry.data[C.CONF_REST_CONTAINER_ID] = "stored-container"
                self.assertTrue(await MODULES.integration.async_migrate_entry(self.hass, self.entry))
                self.assertEqual(self.entry.version, 2)
                self.assertEqual(self.entry.options[C.CONF_USE_REST], expected)
                self.assertEqual(self.entry.options[C.CONF_STREAM_TOPIC], "#")
                self.assertEqual(self.entry.data[C.CONF_REST_REQUEST_STATE], quota)
                self.assertNotIn(C.CONF_REST_CONTAINER_ID, self.entry.data)
                self.assertNotIn(C.CONF_REST_CONTAINER_ID, self.entry.options)

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
        self.entry.options[C.CONF_USE_REST] = False
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
        self.entry.options = {C.CONF_USE_REST: False}
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


class StreamTaskHost:
    """Model HA's distinct startup-tracked and shutdown-cancelled task buckets."""

    def __init__(self):
        self.startup_tasks = set()
        self.background_tasks = set()
        self.async_add_executor_job = AsyncMock(return_value=None)

    def _create_task(self, coroutine, bucket, name=None):
        task = asyncio.create_task(coroutine, name=name)
        bucket.add(task)
        task.add_done_callback(bucket.discard)
        return task

    def async_create_task(self, coroutine):
        return self._create_task(coroutine, self.startup_tasks)

    def async_create_background_task(self, coroutine, name, eager_start=True):
        return self._create_task(coroutine, self.background_tasks, name)

    async def async_block_till_done(self):
        while self.startup_tasks:
            await asyncio.wait(tuple(self.startup_tasks))

    async def async_shutdown(self):
        await self.cancel_tasks(self.background_tasks)

    @staticmethod
    async def cancel_tasks(tasks):
        pending = tuple(tasks)
        for task in pending:
            task.cancel()
        for task in pending:
            try:
                await task
            except asyncio.CancelledError:
                pass


class StreamClient:
    """Keep a simulated MQTT connection open until its task is cancelled."""

    def __init__(self):
        self.messages = self
        self.queue = asyncio.Queue()
        self.subscribed = asyncio.Event()
        self.closed = asyncio.Event()
        self.topic = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.closed.set()
        return False

    async def subscribe(self, topic):
        self.topic = topic
        self.subscribed.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.queue.get()


class StreamLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.hass = StreamTaskHost()
        self.entry_tasks = set()

        def entry_background_task(hass, coroutine, name):
            task = hass.async_create_background_task(coroutine, name)
            self.entry_tasks.add(task)
            task.add_done_callback(self.entry_tasks.discard)
            return task

        self.entry = SimpleNamespace(
            entry_id="stream-test", options={}, data={
                C.CONF_GCID: "account", C.CONF_SCOPE: C.STREAMING_SCOPE,
                C.CONF_STREAM_TOPIC: "#", C.CONF_USE_REST: False,
            },
            async_create_background_task=entry_background_task,
        )
        self.tokens = SimpleNamespace(
            async_get_id_token=AsyncMock(return_value="id-token"),
            async_get_access_token=AsyncMock(return_value="access-token"),
        )
        self.client = StreamClient()
        mqtt = ModuleType("aiomqtt")
        mqtt.Client = Mock(return_value=self.client)
        mqtt.MqttError = type("MqttError", (Exception,), {})
        self.importer = AsyncMock(return_value=mqtt)
        self.import_patch = patch.object(
            MODULES.stream_manager, "async_import_module", self.importer
        )
        self.import_patch.start()
        self.addCleanup(self.import_patch.stop)
        self.mqtt_patch = patch.dict(sys.modules, {"aiomqtt": mqtt})
        self.mqtt_patch.start()
        self.addCleanup(self.mqtt_patch.stop)
        self.manager = MODULES.stream_manager.BmwCarDataStreamManager(
            self.hass, entry=self.entry, token_manager=self.tokens,
        )
        self.addAsyncCleanup(self.manager.async_stop)

    async def test_mqtt_background_stream_does_not_block_startup(self):
        await self.manager.async_start()
        await asyncio.wait_for(self.client.subscribed.wait(), 1)
        task = self.manager._task
        await self.manager.async_start()
        self.assertIs(self.manager._task, task)
        await asyncio.wait_for(self.hass.async_block_till_done(), 1)
        self.assertFalse(task.done())
        self.assertIn(task, self.entry_tasks)
        self.assertEqual(self.client.topic, "account/#")

    async def test_mqtt_background_stream_awaits_nonblocking_module_import(self):
        started = asyncio.Event()
        allowed = asyncio.Event()
        mqtt = self.importer.return_value

        async def deferred_import(_hass, _name):
            started.set()
            await allowed.wait()
            return mqtt

        self.importer.side_effect = deferred_import
        await self.manager.async_start()
        await asyncio.wait_for(started.wait(), 1)
        self.assertFalse(self.client.subscribed.is_set())
        await asyncio.wait_for(self.hass.async_block_till_done(), 1)
        allowed.set()
        await asyncio.wait_for(self.client.subscribed.wait(), 1)
        self.importer.assert_awaited_once_with(self.hass, "aiomqtt")

    async def test_mqtt_background_stream_closes_on_entry_unload_and_ha_shutdown(self):
        for shutdown in (False, True):
            with self.subTest(shutdown=shutdown):
                self.client.subscribed.clear()
                self.client.closed.clear()
                await self.manager.async_start()
                await asyncio.wait_for(self.client.subscribed.wait(), 1)
                task = self.manager._task
                if shutdown:
                    await asyncio.wait_for(self.hass.async_shutdown(), 1)
                else:
                    await asyncio.wait_for(self.hass.cancel_tasks(self.entry_tasks), 1)
                self.assertTrue(task.cancelled())
                self.assertTrue(self.client.closed.is_set())
                await self.manager.async_stop()
                self.assertIsNone(self.manager._task)

    async def test_mqtt_background_stream_stops_promptly_during_reconnect_backoff(self):
        backoff = asyncio.Event()

        async def wait_in_backoff(_seconds):
            backoff.set()
            await asyncio.Event().wait()

        self.manager._async_connect_once = AsyncMock(side_effect=RuntimeError("offline"))
        with patch.object(MODULES.stream_manager.asyncio, "sleep", side_effect=wait_in_backoff):
            await self.manager.async_start()
            await asyncio.wait_for(backoff.wait(), 1)
            await asyncio.wait_for(self.manager.async_stop(), 1)
        self.assertIsNone(self.manager._task)
        self.assertFalse(self.entry_tasks)

    async def test_mqtt_background_stream_delivers_vehicle_and_sensor_updates(self):
        rest_api = SimpleNamespace(async_get_telematic_data=AsyncMock())
        coordinator = MODULES.coordinator.BmwCarDataCoordinator(
            self.hass, entry=self.entry, stream_manager=self.manager,
            rest_api=rest_api, token_manager=self.tokens,
        )
        delivered = asyncio.Event()

        async def on_updates():
            await coordinator.async_apply_stream_snapshot()
            delivered.set()

        self.manager._on_updates = on_updates
        await coordinator.async_initialize()
        await asyncio.wait_for(self.client.subscribed.wait(), 1)
        self.assertEqual(coordinator.data[C.CONF_MAPPINGS], [])
        payload = {"vin": VIN, "telematicData": {
            "soc": {"value": "70"},
            "vehicle.vehicle.travelledDistance": {"value": "12345"},
            "vehicle.cabin.door.lock.status": {"value": "LOCKED"},
            "vehicle.drivetrain.lastRemainingRange": {"value": "500"},
        }}
        await self.client.queue.put(SimpleNamespace(
            topic=SimpleNamespace(value=f"account/{VIN}"),
            payload=json.dumps(payload).encode(),
        ))
        await asyncio.wait_for(delivered.wait(), 1)
        self.assertEqual([mapping["vin"] for mapping in coordinator.data[C.CONF_MAPPINGS]], [VIN])
        for key, expected in (("battery_soc", 70), ("odometer", 12345), ("total_remaining_range", 500)):
            description = next(item for item in MODULES.sensor.VEHICLE_SENSORS if item.key == key)
            sensor = MODULES.sensor.BmwCarDataVehicleSensor(
                coordinator=coordinator, entry=self.entry, vin=VIN, description=description,
            )
            self.assertTrue(sensor.available)
            self.assertEqual(sensor.native_value, expected)
        door = MODULES.binary_sensor.BmwCarDataVehicleBinarySensor(
            coordinator=coordinator, entry=self.entry, vin=VIN,
            description=MODULES.binary_sensor.VEHICLE_BINARY_SENSORS[0],
        )
        self.assertTrue(door.available)
        self.assertTrue(door.is_on)
        rest_api.async_get_telematic_data.assert_not_awaited()
        await asyncio.wait_for(self.hass.async_block_till_done(), 1)


class ApiTests(unittest.IsolatedAsyncioTestCase):
    def response_api(self, body, status=200):
        response = SimpleNamespace(status=status, json=AsyncMock(return_value=body))
        context = Mock(
            __aenter__=AsyncMock(return_value=response),
            __aexit__=AsyncMock(return_value=False),
        )
        session = SimpleNamespace(request=Mock(return_value=context))
        return MODULES.api.BmwCarDataRestApi(session), session

    async def test_list_ignores_inactive_containers_and_rejects_invalid_metadata(self):
        api, _session = self.response_api({"containers": [
            {"containerId": "active", "state": "ACTIVE", "created": "2025-12-01T00:00:00Z"},
            {"containerId": "inactive", "state": "INACTIVE"},
            {"containerId": "deleted", "state": "DELETED"},
        ]})
        self.assertEqual(
            [item["containerId"] for item in await api.async_list_containers(access_token="test")],
            ["active"],
        )
        for invalid in (
            {"containers": {}},
            {"containers": [{"containerId": "bad", "state": "ACTIVE", "created": "invalid"}]},
            {"containers": [{"containerId": "", "state": "ACTIVE", "created": "2025-12-01T00:00:00Z"}]},
        ):
            api, _session = self.response_api(invalid)
            with self.assertRaises(MODULES.api.BmwCarDataApiError):
                await api.async_list_containers(access_token="test")

    async def test_create_posts_only_required_fields_and_requires_returned_id(self):
        api, session = self.response_api({"containerId": "new"}, status=201)
        self.assertEqual(await api.async_create_container(access_token="test"), "new")
        args, kwargs = session.request.call_args
        self.assertEqual(args[0], "POST")
        self.assertEqual(kwargs["json"]["technicalDescriptors"], list(C.REST_SENSOR_KEYS))
        self.assertEqual(kwargs["headers"]["x-version"], "v1")
        api, _session = self.response_api({}, status=201)
        with self.assertRaises(MODULES.api.BmwCarDataApiError):
            await api.async_create_container(access_token="test")

    async def test_new_setup_needs_only_client_id_and_blank_options_topic_becomes_wildcard(self):
        submitted = MODULES.config_flow.STEP_USER_DATA_SCHEMA({"client_id": "test"})
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
            await flow.async_step_user(submitted)
        self.assertEqual(flow._stream_options[C.CONF_STREAM_TOPIC], "#")
        self.assertIn(C.REST_SCOPE, auth.request_device_code.call_args.kwargs["scope"].split())
        self.assertNotIn(C.CONF_REST_CONTAINER_ID, flow._stream_options)
        options = MODULES.config_flow.BmwCarDataOptionsFlow()
        result = await options.async_step_init({
            C.CONF_STREAM_TOPIC: "  ", C.CONF_USE_REST: True,
        })
        self.assertEqual(result["data"][C.CONF_STREAM_TOPIC], "#")
        self.assertTrue(result["data"][C.CONF_USE_REST])

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
                session = SimpleNamespace(request=Mock(return_value=context))
                api = MODULES.api.BmwCarDataRestApi(session)
                call = api.async_get_telematic_data(
                    access_token="test-token", vin=VIN, container_id="container & test",
                )
                if expected_error:
                    with self.assertRaises(expected_error):
                        await call
                else:
                    self.assertEqual(await call, body["telematicData"])
                    args, kwargs = session.request.call_args
                    self.assertIn("containerId=container+%26+test", args[1])
                    self.assertEqual(kwargs["headers"]["x-version"], "v1")
                    self.assertEqual(kwargs["headers"]["Authorization"], "Bearer test-token")
                if status >= 400:
                    response.json.assert_not_awaited()

    async def test_setup_requests_rest_scope_only_when_enabled(self):
        for use_rest in (False, True):
            with self.subTest(use_rest=use_rest):
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
                        C.CONF_USE_REST: use_rest,
                    })
                scopes = auth.request_device_code.call_args.kwargs["scope"].split()
                self.assertIn(C.STREAMING_SCOPE, scopes)
                self.assertEqual(C.REST_SCOPE in scopes, use_rest)

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
                flow._stream_options = {C.CONF_USE_REST: True}
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
