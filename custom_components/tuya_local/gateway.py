"""
API for Tuya Local devices.
"""

import asyncio
import logging
from threading import Lock

import tinytuya
from homeassistant.const import (
    EVENT_HOMEASSISTANT_STARTED,
    EVENT_HOMEASSISTANT_STOP,
)
from homeassistant.core import HomeAssistant

from .const import (
    API_PROTOCOL_VERSIONS,
)
from .helpers.log import log_json

_LOGGER = logging.getLogger(__name__)


class TuyaLocalGateway(object):
    def __init__(
        self,
        dev_id,
        address,
        local_key,
        protocol_version,
        hass: HomeAssistant,
    ):
        """
        Represents a Tuya-based gateway.

        Args:
            dev_id (str): The device id.
            address (str): The network address.
            local_key (str): The encryption key.
            protocol_version (str | number): The protocol version.
            dev_cid (str): The sub device id.
            hass (HomeAssistant): The Home Assistant instance.
            poll_only (bool): True if the device should be polled only
        """
        self._subdevices = []
        self._running = False
        self._shutdown_listener = None
        self._startup_listener = None
        try:
            self._api = tinytuya.Device(dev_id, address, local_key, persist=True)
        except Exception as e:
            _LOGGER.error(
                "%s: %s while initialising gateway %s",
                type(e),
                e,
                dev_id,
            )
            raise e

        # we handle retries at a higher level so we can rotate protocol version
        self._api.set_socketRetryLimit(1)

        self._refresh_task = None
        self._protocol_configured = protocol_version
        self._hass = hass

        # API calls to update Tuya devices are asynchronous and non-blocking.
        # This means you can send a change and immediately request an updated
        # state (like HA does), but because it has not yet finished processing
        # you will be returned the old state.
        # The solution is to keep a temporary list of changed properties that
        # we can overlay onto the state while we wait for the board to update
        # its switches.
        self._FAKE_IT_TIMEOUT = 5
        self._CACHE_TIMEOUT = 30
        # More attempts are needed in auto mode so we can cycle through all
        # the possibilities a couple of times
        self._AUTO_CONNECTION_ATTEMPTS = len(API_PROTOCOL_VERSIONS) * 2 + 1
        self._SINGLE_PROTO_CONNECTION_ATTEMPTS = 3
        # The number of failures from a working protocol before retrying other protocols.
        self._AUTO_FAILURE_RESET_COUNT = 10
        self._lock = Lock()
        self.start()

    @property
    def unique_id(self):
        """Return the unique id for this device (the dev_id or dev_cid)."""
        return self._api.id

    def actually_start(self, event=None):
        _LOGGER.debug("Starting monitor loop for gateway %s", self._api.id)
        self._running = True
        self._shutdown_listener = self._hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP, self.async_stop
        )
        self._refresh_task = self._hass.async_create_task(self.receive_loop())

    def start(self):
        if self._hass.is_stopping:
            return
        elif self._hass.is_running:
            if self._startup_listener:
                self._startup_listener()
                self._startup_listener = None
            self.actually_start()
        else:
            self._startup_listener = self._hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_STARTED, self.actually_start
            )

    async def async_stop(self, event=None):
        _LOGGER.debug("Stopping monitor loop for gateway %s", self._api.id)
        self._running = False
        if self._shutdown_listener:
            self._shutdown_listener()
            self._shutdown_listener = None
        if self._refresh_task:
            await self._refresh_task
        _LOGGER.debug("Monitor loop for gateway %s stopped", self._api.id)
        self._refresh_task = None

    async def receive_loop(self):
        """Coroutine wrapper for async_receive generator."""
        try:
            async for poll in self.async_receive():
                if type(poll) is dict:
                    if "device" in poll:
                        for subdevice in self._subdevices:
                            if subdevice._api == poll["device"]:
                                if "data" in poll:
                                    poll = poll["data"]
                                    if "dps" in poll:
                                        poll = poll["dps"]
                                    subdevice.update_entities(poll)
                else:
                    _LOGGER.debug(
                        "gateway %s received non data %s",
                        self._api.id,
                        log_json(poll),
                    )
            _LOGGER.warning("gateway %s receive loop has terminated", self._api.id)

        except Exception as t:
            _LOGGER.exception(
                "gateway %s receive loop terminated by exception %s", self._api.id, t
            )

    def pause(self):
        self._temporary_poll = True

    def resume(self):
        self._temporary_poll = False

    async def async_receive(self):
        """Receive messages from a persistent connection asynchronously."""
        while self._running:
            try:
                await self._hass.async_add_executor_job(
                    self._api.heartbeat,
                    True,
                )
                poll = await self._hass.async_add_executor_job(
                    self._api.receive,
                )

                if poll:
                    if "Error" in poll:
                        _LOGGER.warning(
                            "%s error reading gateway: %s", self._api.id, poll["Error"]
                        )
                        if "Payload" in poll and poll["Payload"]:
                            _LOGGER.info(
                                "%s err payload: %s",
                                self._api.id,
                                poll["Payload"],
                            )
                    else:
                        _LOGGER.debug(
                            "%s received: %s",
                            self._api.id,
                            poll,
                        )
                        yield poll

            except asyncio.CancelledError:
                self._running = False
                # Close the persistent connection when exiting the loop
                self._api.set_socketPersistent(False)
                raise
            except Exception as t:
                _LOGGER.exception(
                    "gateway %s receive loop error %s:%s",
                    self._api.id,
                    type(t),
                    t,
                )
                await asyncio.sleep(5)

        # Close the persistent connection when exiting the loop
        self._api.set_socketPersistent(False)
