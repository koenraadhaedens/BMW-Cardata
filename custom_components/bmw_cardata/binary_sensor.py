"""Binary sensor platform for BMW CarData."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    CONF_BASIC_DATA_BY_VIN,
    CONF_MAPPINGS,
    CONF_TELEMATIC_DATA_BY_VIN,
    DATA_ENTRIES,
    DOMAIN,
)
from .coordinator import BmwCarDataCoordinator


@dataclass(frozen=True, kw_only=True)
class VehicleBinarySensorDescription(BinarySensorEntityDescription):
    """Description for VIN-level binary sensors."""

    value_fn: Callable[
        [dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]],
        bool | None,
    ]


VEHICLE_BINARY_SENSORS: tuple[VehicleBinarySensorDescription, ...] = (
    VehicleBinarySensorDescription(
        key="doors_closed",
        translation_key="doors_closed",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _car_locked_from_telematic(telematic),
    ),
    VehicleBinarySensorDescription(
        key="is_charging",
        translation_key="is_charging",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda _mapping, _basic, telematic: _is_vehicle_charging_from_telematic(telematic),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up BMW CarData binary sensors from config entry."""
    coordinator: BmwCarDataCoordinator = hass.data[DOMAIN][DATA_ENTRIES][entry.entry_id][
        "coordinator"
    ]

    known_unique_ids: set[str] = set()

    def _add_vin_entities() -> None:
        mappings = coordinator.data.get(CONF_MAPPINGS, []) if coordinator.data else []

        new_entities: list[BinarySensorEntity] = []
        for mapping in mappings:
            vin = mapping.get("vin")
            if not isinstance(vin, str) or not vin:
                continue

            for description in VEHICLE_BINARY_SENSORS:
                unique_id = f"{entry.entry_id}_{vin}_{description.key}"
                if unique_id in known_unique_ids:
                    continue
                known_unique_ids.add(unique_id)
                new_entities.append(
                    BmwCarDataVehicleBinarySensor(
                        coordinator=coordinator,
                        entry=entry,
                        vin=vin,
                        description=description,
                    )
                )

        if new_entities:
            async_add_entities(new_entities)

    _add_vin_entities()
    entry.async_on_unload(coordinator.async_add_listener(_add_vin_entities))


class BmwCarDataVehicleBinarySensor(
    CoordinatorEntity[BmwCarDataCoordinator], BinarySensorEntity
):
    """VIN-level BMW CarData binary sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        *,
        coordinator: BmwCarDataCoordinator,
        entry: ConfigEntry,
        vin: str,
        description: VehicleBinarySensorDescription,
    ) -> None:
        """Initialize VIN-level binary sensor."""
        super().__init__(coordinator)
        self.entity_description = description
        self._vin = vin
        self._attr_unique_id = f"{entry.entry_id}_{vin}_{description.key}"
        self._attr_name = f"{vin} {description.key.replace('_', ' ').title()}"

    @property
    def is_on(self) -> bool | None:
        """Return binary sensor state."""
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


def _car_locked_from_telematic(telematic_data: dict[str, dict[str, Any]]) -> bool | None:
    """Derive lock state from door lock telematic entries.

    Returns:
    - True when vehicle is locked/secured
    - False when vehicle is unlocked
    - None when lock state is unavailable
    """
    lock_entry = telematic_data.get("vehicle.cabin.door.lock.status")
    if isinstance(lock_entry, dict):
        parsed = _normalize_lock_state(lock_entry.get("value"))
        if parsed is not None:
            return parsed

    door_status_entry = telematic_data.get("vehicle.cabin.door.status")
    if isinstance(door_status_entry, dict):
        parsed = _normalize_door_status_value(door_status_entry.get("value"))
        if parsed is not None:
            return parsed

    for key, entry in telematic_data.items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            continue
        key_l = key.lower()
        if "door" not in key_l or "lock" not in key_l:
            continue
        parsed = _normalize_lock_state(entry.get("value"))
        if parsed is not None:
            return parsed

    return None


def _normalize_lock_state(value: Any) -> bool | None:
    """Normalize lock state to bool.

    True means locked, False means unlocked.
    """
    if isinstance(value, str):
        lowered = value.strip().lower().replace("-", "").replace("_", "")
        if lowered in {
            "secured",
            "locked",
            "selectivelocked",
            "closedlocked",
            "asnsecured",
            "asnlocked",
            "asnistrue",
        }:
            return True
        if lowered in {
            "unlocked",
            "open",
            "opened",
            "notlocked",
            "asnunlocked",
            "asnisfalse",
        }:
            return False
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _normalize_door_status_value(value: Any) -> bool | None:
    """Extract lock state from `vehicle.cabin.door.status` payload variants."""
    if isinstance(value, dict):
        # Prefer explicit aggregate booleans when available.
        for key in ("allDoorsLocked", "all_doors_locked"):
            if key in value:
                parsed = _normalize_lock_state(value.get(key))
                if parsed is not None:
                    return parsed

        for key in ("newDoorStatus", "oldDoorStatus", "doorStatus", "status"):
            if key in value:
                parsed = _normalize_lock_state(value.get(key))
                if parsed is not None:
                    return parsed

        # Some payloads are nested one level deeper.
        for nested in value.values():
            parsed = _normalize_door_status_value(nested)
            if parsed is not None:
                return parsed

    return _normalize_lock_state(value)


def _is_vehicle_charging_from_telematic(telematic_data: dict[str, dict[str, Any]]) -> bool | None:
    """Derive charging activity state from EV charging telematic entries."""
    preferred_keys = (
        "vehicle.drivetrain.electricEngine.charging.status",
        "vehicle.drivetrain.electricEngine.charging.hvStatus",
        "vehicle.drivetrain.electricEngine.charging",
        "vehicle.drivetrain.electricEngine.charging.chargingMode",
    )
    for key in preferred_keys:
        entry = telematic_data.get(key)
        if isinstance(entry, dict):
            parsed = _normalize_charging_state(entry.get("value"))
            if parsed is not None:
                return parsed

    for key, entry in telematic_data.items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            continue
        key_l = key.lower()
        if "electricengine" not in key_l or "charging" not in key_l:
            continue
        if "status" not in key_l and "hvstatus" not in key_l:
            continue
        parsed = _normalize_charging_state(entry.get("value"))
        if parsed is not None:
            return parsed

    return None


def _normalize_charging_state(value: Any) -> bool | None:
    """Normalize charging state to bool.

    True means active charging, False means not charging.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower().replace("-", "_").replace(" ", "_")
        active_states = {
            "charging",
            "chargingactive",
            "charging_active",
            "charging_in_timeslot",
            "route_optimized_charging_in_progress",
        }
        inactive_states = {
            "nocharging",
            "no_charging",
            "not_charging",
            "chargingpaused",
            "charging_paused",
            "chargingended",
            "charging_ended",
            "finished_fully_charged",
            "finished_not_full",
            "error",
            "unknown",
            "invalid",
        }
        if lowered in active_states:
            return True
        if lowered in inactive_states:
            return False
        if "charging" in lowered and "not" not in lowered and "no_" not in lowered:
            return True
        if "not_charging" in lowered or "no_charging" in lowered:
            return False
        return None

    if isinstance(value, dict):
        for key in ("status", "hvStatus", "chargingStatus", "state"):
            if key in value:
                parsed = _normalize_charging_state(value.get(key))
                if parsed is not None:
                    return parsed
        for nested in value.values():
            parsed = _normalize_charging_state(nested)
            if parsed is not None:
                return parsed

    return None
