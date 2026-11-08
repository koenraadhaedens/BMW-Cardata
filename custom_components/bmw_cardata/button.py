"""Manual REST refresh buttons for BMW CarData."""

from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_MAPPINGS, DATA_ENTRIES, DOMAIN
from .coordinator import BmwCarDataCoordinator
from .entity import BmwCarDataVehicleEntity


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Add a refresh button for each REST-enabled, discovered vehicle."""
    coordinator: BmwCarDataCoordinator = hass.data[DOMAIN][DATA_ENTRIES][entry.entry_id][
        "coordinator"
    ]
    known_vins: set[str] = set()

    def _add_vin_entities() -> None:
        if not coordinator.use_rest:
            return
        mappings = (coordinator.data or {}).get(CONF_MAPPINGS, [])
        entities: list[ButtonEntity] = []
        for mapping in mappings:
            vin = mapping.get("vin")
            if not isinstance(vin, str) or not vin or vin in known_vins:
                continue
            known_vins.add(vin)
            entities.append(
                BmwCarDataRestRefreshButton(coordinator=coordinator, entry=entry, vin=vin)
            )
        if entities:
            async_add_entities(entities)

    _add_vin_entities()
    entry.async_on_unload(coordinator.async_add_listener(_add_vin_entities))


class BmwCarDataRestRefreshButton(BmwCarDataVehicleEntity, ButtonEntity):
    """Request BMW's latest stored battery and charging readings for one VIN."""

    _attr_icon = "mdi:refresh"
    _attr_entity_category = EntityCategory.CONFIG
    _attr_translation_key = "refresh_rest"

    def __init__(
        self, *, coordinator: BmwCarDataCoordinator, entry: ConfigEntry, vin: str
    ) -> None:
        """Initialize the VIN-level button, independently of REST sensor availability."""
        super().__init__(coordinator, vin)
        self._attr_unique_id = f"{entry.entry_id}_{vin}_refresh_rest"

    @property
    def available(self) -> bool:
        """Keep manual recovery possible when a prior REST read failed."""
        return self.coordinator.use_rest and super().available

    async def async_press(self) -> None:
        """Bypass this vehicle's cooldown but never bypass the daily quota."""
        await self.coordinator.async_manual_refresh_rest(self._vin)
