"""Device tracker platform for BMW CarData."""

from __future__ import annotations

from typing import Any

from homeassistant.components.device_tracker import SourceType, TrackerEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    CONF_MAPPINGS,
    CONF_TELEMATIC_DATA_BY_VIN,
    DATA_ENTRIES,
    DOMAIN,
)
from .coordinator import BmwCarDataCoordinator
from .entity import BmwCarDataVehicleEntity
from .sensor import _find_numeric_telematic_value


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up BMW CarData device trackers from config entry."""
    coordinator: BmwCarDataCoordinator = hass.data[DOMAIN][DATA_ENTRIES][entry.entry_id][
        "coordinator"
    ]

    known_unique_ids: set[str] = set()

    def _add_vin_entities() -> None:
        mappings = coordinator.data.get(CONF_MAPPINGS, []) if coordinator.data else []

        new_entities: list[TrackerEntity] = []
        for mapping in mappings:
            vin = mapping.get("vin")
            if not isinstance(vin, str) or not vin:
                continue

            unique_id = f"{entry.entry_id}_{vin}_tracker"
            if unique_id in known_unique_ids:
                continue
            known_unique_ids.add(unique_id)
            new_entities.append(
                BmwCarDataVehicleTracker(
                    coordinator=coordinator,
                    entry=entry,
                    vin=vin,
                )
            )

        if new_entities:
            async_add_entities(new_entities)

    _add_vin_entities()
    entry.async_on_unload(coordinator.async_add_listener(_add_vin_entities))


class BmwCarDataVehicleTracker(BmwCarDataVehicleEntity, TrackerEntity):
    """GPS device tracker for a BMW vehicle."""

    _attr_icon = "mdi:car"
    _attr_translation_key = "location"

    def __init__(
        self,
        *,
        coordinator: BmwCarDataCoordinator,
        entry: ConfigEntry,
        vin: str,
    ) -> None:
        """Initialize tracker."""
        super().__init__(coordinator, vin)
        self._attr_unique_id = f"{entry.entry_id}_{vin}_tracker"

    @property
    def source_type(self) -> SourceType:
        """Return GPS as source type."""
        return SourceType.GPS

    def _get_telematic(self) -> dict[str, Any]:
        """Return telematic data dict for this VIN."""
        telematic_data_by_vin = (
            self.coordinator.data.get(CONF_TELEMATIC_DATA_BY_VIN, {})
            if self.coordinator.data
            else {}
        )
        telematic = telematic_data_by_vin.get(self._vin, {})
        return telematic if isinstance(telematic, dict) else {}

    @property
    def latitude(self) -> float | None:
        """Return vehicle latitude."""
        return _find_numeric_telematic_value(
            self._get_telematic(),
            (
                ("currentlocation", "latitude"),
                ("current", "location", "latitude"),
                ("position", "latitude"),
                ("latitude",),
            ),
        )

    @property
    def longitude(self) -> float | None:
        """Return vehicle longitude."""
        return _find_numeric_telematic_value(
            self._get_telematic(),
            (
                ("currentlocation", "longitude"),
                ("current", "location", "longitude"),
                ("position", "longitude"),
                ("longitude",),
            ),
        )

    @property
    def location_accuracy(self) -> int:
        """Return GPS accuracy in metres (BMW does not provide this)."""
        return 0

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        return {"vin": self._vin}
