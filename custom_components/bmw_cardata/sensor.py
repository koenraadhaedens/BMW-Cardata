"""Sensor platform for BMW CarData."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import SensorEntity, SensorEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    CONF_ACTIVE_CONTAINER_ID,
    CONF_BASIC_DATA_BY_VIN,
    CONF_MAPPINGS,
    CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED,
    CONF_TELEMATIC_DATA_BY_VIN,
    DATA_ENTRIES,
    DOMAIN,
)
from .coordinator import BmwCarDataCoordinator


@dataclass(frozen=True, kw_only=True)
class VehicleSensorDescription(SensorEntityDescription):
    """Description of a VIN-level sensor."""

    value_fn: Callable[
        [dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]],
        str | int | float | None,
    ]


VEHICLE_SENSORS: tuple[VehicleSensorDescription, ...] = (
    VehicleSensorDescription(
        key="odometer",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_numeric_telematic_value(
            telematic,
            (("travelled", "distance"), ("travelleddistance",)),
            exact_keys=("vehicle.vehicle.travelledDistance",),
        ),
    ),
    VehicleSensorDescription(
        key="fuel_level",
        translation_key="fuel_level",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _round_percent_if_applicable(
            _find_numeric_telematic_value(
                telematic,
                (
                    ("fuel", "level"),
                    ("fuel", "filling"),
                    ("fuel", "percent"),
                    ("tank", "level"),
                    ("fuel",),
                ),
                excluded_terms=("consumption", "economy", "average"),
            )
        ),
    ),
    VehicleSensorDescription(
        key="fuel_remaining",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_numeric_telematic_value(
            telematic,
            (("remaining", "fuel"),),
            exact_keys=("vehicle.drivetrain.fuelSystem.remainingFuel",),
        ),
    ),
    VehicleSensorDescription(
        key="remaining_range",
        translation_key="remaining_range",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_numeric_telematic_value(
            telematic,
            (
                ("remaining", "range"),
                ("cruising", "range"),
                ("range",),
            ),
            excluded_terms=("charge", "time", "duration"),
        ),
    ),
    VehicleSensorDescription(
        key="electric_remaining_range",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_numeric_telematic_value(
            telematic,
            (("kombi", "remaining", "electric", "range"),),
            exact_keys=("vehicle.drivetrain.electricEngine.kombiRemainingElectricRange",),
        ),
    ),
    VehicleSensorDescription(
        key="battery_max_energy",
        entity_category=EntityCategory.DIAGNOSTIC,
        native_unit_of_measurement="kWh",
        value_fn=lambda _mapping, _basic, telematic: _find_numeric_telematic_value(
            telematic,
            (("battery", "management", "max", "energy"),),
            exact_keys=("vehicle.drivetrain.batteryManagement.maxEnergy",),
        ),
    ),
    VehicleSensorDescription(
        key="charging_status",
        translation_key="charging_status",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_text_telematic_value(
            telematic,
            exact_keys=(
                "vehicle.drivetrain.electricEngine.charging",
                "vehicle.drivetrain.electricEngine.charging.status",
                "vehicle.drivetrain.electricEngine.charging.hvStatus",
                "vehicle.drivetrain.electricEngine.charging.connectorStatus",
            ),
            include_term_groups=(
                ("electricengine", "charging", "status"),
                ("electricengine", "charging", "hvstatus"),
            ),
        ),
    ),
    VehicleSensorDescription(
        key="current_latitude",
        translation_key="current_latitude",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_numeric_telematic_value(
            telematic,
            (
                ("currentlocation", "latitude"),
                ("current", "location", "latitude"),
                ("position", "latitude"),
                ("latitude",),
            ),
        ),
    ),
    VehicleSensorDescription(
        key="current_longitude",
        translation_key="current_longitude",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_numeric_telematic_value(
            telematic,
            (
                ("currentlocation", "longitude"),
                ("current", "location", "longitude"),
                ("position", "longitude"),
                ("longitude",),
            ),
        ),
    ),
    VehicleSensorDescription(
        key="sunroof_status",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_text_telematic_value(
            telematic,
            exact_keys=("vehicle.cabin.sunroof.status",),
        ),
    ),
    VehicleSensorDescription(
        key="sunroof_tilt_status",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_text_telematic_value(
            telematic,
            exact_keys=("vehicle.cabin.sunroof.tiltStatus",),
        ),
    ),
    VehicleSensorDescription(
        key="sunroof_overall_status",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _find_text_telematic_value(
            telematic,
            exact_keys=("vehicle.cabin.sunroof.overallStatus",),
        ),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up BMW CarData sensors from config entry."""
    coordinator: BmwCarDataCoordinator = hass.data[DOMAIN][DATA_ENTRIES][entry.entry_id][
        "coordinator"
    ]

    entities: list[SensorEntity] = [
        BmwCarDataMappedVehiclesSensor(coordinator=coordinator, entry=entry),
        BmwCarDataStreamStartupBootstrapSensor(coordinator=coordinator, entry=entry),
    ]

    known_unique_ids: set[str] = set()

    def _add_vin_entities() -> None:
        mappings = coordinator.data.get(CONF_MAPPINGS, []) if coordinator.data else []
        basic_data_by_vin = (
            coordinator.data.get(CONF_BASIC_DATA_BY_VIN, {}) if coordinator.data else {}
        )

        new_entities: list[SensorEntity] = []
        for mapping in mappings:
            vin = mapping.get("vin")
            if not isinstance(vin, str) or not vin:
                continue

            for description in VEHICLE_SENSORS:
                unique_id = f"{entry.entry_id}_{vin}_{description.key}"
                if unique_id in known_unique_ids:
                    continue
                known_unique_ids.add(unique_id)
                new_entities.append(
                    BmwCarDataVehicleSensor(
                        coordinator=coordinator,
                        entry=entry,
                        vin=vin,
                        description=description,
                    )
                )

        if new_entities:
            async_add_entities(new_entities)

    _add_vin_entities()
    async_add_entities(entities)

    entry.async_on_unload(coordinator.async_add_listener(_add_vin_entities))


class BmwCarDataMappedVehiclesSensor(CoordinatorEntity[BmwCarDataCoordinator], SensorEntity):
    """Sensor for mapped vehicles count."""

    _attr_icon = "mdi:car-multiple"
    _attr_has_entity_name = True
    _attr_name = "Mapped Vehicles"
    _attr_translation_key = "mapped_vehicles"

    def __init__(self, *, coordinator: BmwCarDataCoordinator, entry: ConfigEntry) -> None:
        """Initialize sensor."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{entry.entry_id}_mapped_vehicles"

    @property
    def native_value(self) -> int:
        """Return count of mapped vehicles."""
        mappings = self.coordinator.data.get(CONF_MAPPINGS, []) if self.coordinator.data else []
        return len(mappings)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return sensor attributes."""
        mappings = self.coordinator.data.get(CONF_MAPPINGS, []) if self.coordinator.data else []
        vins = [mapping.get("vin") for mapping in mappings if isinstance(mapping.get("vin"), str)]
        telematic_data_by_vin = (
            self.coordinator.data.get(CONF_TELEMATIC_DATA_BY_VIN, {}) if self.coordinator.data else {}
        )
        active_container_id = (
            self.coordinator.data.get(CONF_ACTIVE_CONTAINER_ID) if self.coordinator.data else None
        )
        key_counts: dict[str, int] = {}
        sample_keys: dict[str, list[str]] = {}
        if isinstance(telematic_data_by_vin, dict):
            for vin, values in telematic_data_by_vin.items():
                if not isinstance(vin, str) or not isinstance(values, dict):
                    continue
                keys = [key for key in values.keys() if isinstance(key, str)]
                key_counts[vin] = len(keys)
                sample_keys[vin] = keys[:10]
        return {
            "vins": vins,
            "active_container_id": active_container_id,
            "streaming_enabled": self.coordinator.use_streaming,
            "stream_startup_bootstrap_completed": self.coordinator.data.get(
                CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED,
                False,
            )
            if self.coordinator.data
            else False,
            "telematic_key_counts": key_counts,
            "telematic_sample_keys": sample_keys,
        }


class BmwCarDataStreamStartupBootstrapSensor(
    CoordinatorEntity[BmwCarDataCoordinator], SensorEntity
):
    """Diagnostic sensor showing startup REST bootstrap state in streaming mode."""

    _attr_icon = "mdi:database-sync-outline"
    _attr_has_entity_name = True
    _attr_name = "Stream Startup Bootstrap"
    _attr_translation_key = "stream_startup_bootstrap"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, *, coordinator: BmwCarDataCoordinator, entry: ConfigEntry) -> None:
        """Initialize sensor."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{entry.entry_id}_stream_startup_bootstrap"

    @property
    def native_value(self) -> str:
        """Return startup bootstrap state."""
        completed = (
            self.coordinator.data.get(CONF_STREAM_STARTUP_BOOTSTRAP_COMPLETED, False)
            if self.coordinator.data
            else False
        )
        return "completed" if bool(completed) else "pending"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        return {
            "streaming_enabled": self.coordinator.use_streaming,
            "completed": self.native_value == "completed",
        }


class BmwCarDataVehicleSensor(CoordinatorEntity[BmwCarDataCoordinator], SensorEntity):
    """VIN-level BMW CarData sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        *,
        coordinator: BmwCarDataCoordinator,
        entry: ConfigEntry,
        vin: str,
        description: VehicleSensorDescription,
    ) -> None:
        """Initialize VIN-level sensor."""
        super().__init__(coordinator)
        self.entity_description = description
        self._vin = vin
        self._attr_unique_id = f"{entry.entry_id}_{vin}_{description.key}"
        self._attr_name = f"{vin} {description.key.replace('_', ' ').title()}"

    @property
    def native_value(self) -> str | int | float | None:
        """Return sensor state."""
        mappings = self.coordinator.data.get(CONF_MAPPINGS, []) if self.coordinator.data else []
        basic_data_by_vin = (
            self.coordinator.data.get(CONF_BASIC_DATA_BY_VIN, {}) if self.coordinator.data else {}
        )
        telematic_data_by_vin = (
            self.coordinator.data.get(CONF_TELEMATIC_DATA_BY_VIN, {}) if self.coordinator.data else {}
        )

        mapping = next(
            (
                item
                for item in mappings
                if isinstance(item, dict) and item.get("vin") == self._vin
            ),
            {},
        )
        basic_data = basic_data_by_vin.get(self._vin, {})
        telematic_data = telematic_data_by_vin.get(self._vin, {})
        if not isinstance(mapping, dict):
            mapping = {}
        if not isinstance(basic_data, dict):
            basic_data = {}
        if not isinstance(telematic_data, dict):
            telematic_data = {}
        return self.entity_description.value_fn(mapping, basic_data, telematic_data)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        return {"vin": self._vin}


def _find_numeric_telematic_value(
    telematic_data: dict[str, dict[str, Any]],
    include_term_groups: tuple[tuple[str, ...], ...],
    exact_keys: tuple[str, ...] = (),
    excluded_terms: tuple[str, ...] = (),
) -> float | None:
    """Find numeric telematic value by prioritized key-term groups."""
    normalized_items: list[tuple[str, str, dict[str, Any]]] = [
        (key, key.lower(), value)
        for key, value in telematic_data.items()
        if isinstance(key, str) and isinstance(value, dict)
    ]

    exact_keys_lower = {candidate.lower() for candidate in exact_keys}
    if exact_keys_lower:
        for _original_key, key_l, value in normalized_items:
            if key_l not in exact_keys_lower:
                continue
            if any(excluded in key_l for excluded in excluded_terms):
                continue
            numeric = _to_float(value.get("value"))
            if numeric is not None:
                return numeric

    for terms in include_term_groups:
        for _original_key, key_l, value in normalized_items:
            if any(excluded in key_l for excluded in excluded_terms):
                continue
            if all(term in key_l for term in terms):
                numeric = _to_float(value.get("value"))
                if numeric is not None:
                    return numeric
    return None


def _find_battery_level_value(telematic_data: dict[str, dict[str, Any]]) -> float | None:
    """Find battery SOC with strong preference for EV/HV battery keys."""
    return _find_numeric_telematic_value(
        telematic_data,
        (
            ("powertrain", "electric", "battery", "displayedsoc"),
            ("powertrain", "electric", "battery", "soc"),
            ("powertrain", "electric", "battery", "state", "of", "charge"),
            ("hvs", "soc"),
            ("hvb", "soc"),
            ("high", "voltage", "battery", "soc"),
            ("highvoltage", "battery", "stateofcharge"),
            ("electric", "battery", "stateofcharge"),
        ),
        exact_keys=(
            "vehicle.powertrain.electric.battery.displayedSoc",
            "vehicle.powertrain.electric.battery.stateOfCharge",
            "vehicle.powertrain.electric.battery.remainingChargePercent",
            "vehicle.powertrain.electric.battery.chargeLevel",
            "vehicle.powertrain.highVoltageBattery.stateOfCharge",
            "vehicle.powertrain.highVoltageBattery.displayedSoc",
        ),
        excluded_terms=(
            "12v",
            "starter",
            "aux",
            "auxiliary",
        ),
    )


def _find_text_telematic_value(
    telematic_data: dict[str, dict[str, Any]],
    *,
    exact_keys: tuple[str, ...] = (),
    include_term_groups: tuple[tuple[str, ...], ...] = (),
    excluded_terms: tuple[str, ...] = (),
) -> str | None:
    """Find text telematic value by exact key or included terms."""
    normalized_items: list[tuple[str, dict[str, Any]]] = [
        (key.lower(), value)
        for key, value in telematic_data.items()
        if isinstance(key, str) and isinstance(value, dict)
    ]

    exact_keys_lower = {candidate.lower() for candidate in exact_keys}
    if exact_keys_lower:
        for key_l, value in normalized_items:
            if key_l not in exact_keys_lower:
                continue
            if any(excluded in key_l for excluded in excluded_terms):
                continue
            extracted = _extract_text_value(value.get("value"))
            if extracted is not None:
                return extracted

    for terms in include_term_groups:
        for key_l, value in normalized_items:
            if any(excluded in key_l for excluded in excluded_terms):
                continue
            if all(term in key_l for term in terms):
                extracted = _extract_text_value(value.get("value"))
                if extracted is not None:
                    return extracted
    return None


def _extract_text_value(value: Any) -> str | None:
    """Extract a representative text value from string or nested payloads."""
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None

    if isinstance(value, dict):
        # Prefer commonly used status keys first.
        preferred_keys = (
            "status",
            "hvStatus",
            "connectorStatus",
            "chargingStatus",
            "state",
            "value",
        )
        for key in preferred_keys:
            if key not in value:
                continue
            extracted = _extract_text_value(value.get(key))
            if extracted is not None:
                return extracted

        # Fallback: search one level deeper for any textual leaf.
        for nested in value.values():
            extracted = _extract_text_value(nested)
            if extracted is not None:
                return extracted

    return None


def _to_float(value: Any) -> float | None:
    """Convert telematic value to float if possible."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", ".")
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _round_percent_if_applicable(value: float | None) -> int | float | None:
    """Round probable percentage values to whole numbers."""
    if value is None:
        return None
    if 0 <= value <= 100:
        return int(value + 0.5)
    return value
