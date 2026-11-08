"""Shared vehicle identity for BMW CarData entities."""

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import BmwCarDataCoordinator


class BmwCarDataVehicleEntity(CoordinatorEntity[BmwCarDataCoordinator]):
    """Associate all platforms with the same vehicle device."""

    _attr_has_entity_name = True

    def __init__(self, coordinator: BmwCarDataCoordinator, vin: str) -> None:
        """Initialize the vehicle identity."""
        super().__init__(coordinator)
        self._vin = vin
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, vin)},
            name=f"BMW {vin}",
            manufacturer="BMW",
        )
