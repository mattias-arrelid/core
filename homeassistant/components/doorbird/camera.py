"""Support for viewing the camera feed from a DoorBird video doorbell."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
import datetime as dt
import logging
from typing import override

import aiohttp
from aiohttp import encode_basic_auth
from yarl import URL

from homeassistant.components.camera import (
    Camera,
    CameraEntityCapabilities,
    CameraEntityFeature,
    CameraStreamMedia,
    CameraStreamSource,
)
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .deprecation import async_should_create_deprecated_entity
from .entity import DoorBirdEntity
from .models import DoorBirdConfigEntry, DoorBirdData
from .util import get_mac_address_from_door_station_info

_LAST_VISITOR_INTERVAL = dt.timedelta(minutes=2)
_LAST_MOTION_INTERVAL = dt.timedelta(seconds=30)
_LIVE_INTERVAL = dt.timedelta(seconds=45)
_LOGGER = logging.getLogger(__name__)
_TIMEOUT = 15  # seconds
_TALK_BACK_REFUSAL_TIMEOUT = 0.5  # seconds


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: DoorBirdConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up the DoorBird camera platform."""
    door_bird_data = config_entry.runtime_data
    device = door_bird_data.door_station.device
    mac_addr = get_mac_address_from_door_station_info(door_bird_data.door_station_info)
    entity_registry = er.async_get(hass)

    entities = [
        DoorBirdCamera(
            door_bird_data,
            device.live_image_url,
            "live",
            _LIVE_INTERVAL,
            device.rtsp_live_video_url,
        ),
        # Not every model serves the 1080p stream
        DoorBirdCamera(
            door_bird_data,
            device.live_image_url,
            "live_1080p",
            _LIVE_INTERVAL,
            device.rtsp_live_video_1080p_url,
            enabled_default=False,
        ),
    ]

    for camera_id, history_type, interval in (
        ("last_ring", "doorbell", _LAST_VISITOR_INTERVAL),
        ("last_motion", "motionsensor", _LAST_MOTION_INTERVAL),
    ):
        if async_should_create_deprecated_entity(
            hass,
            entity_registry,
            config_entry_id=config_entry.entry_id,
            platform_domain=Platform.CAMERA,
            entity_unique_id=f"{mac_addr}_{camera_id}",
            issue_id=f"deprecated_camera_{mac_addr}_{camera_id}",
            translation_key=f"deprecated_camera_{camera_id}",
        ):
            entities.append(
                DoorBirdCamera(
                    door_bird_data,
                    device.history_image_url(1, history_type),
                    camera_id,
                    interval,
                )
            )

    async_add_entities(entities)


class DoorBirdCamera(DoorBirdEntity, Camera):
    """The camera on a DoorBird device."""

    def __init__(
        self,
        door_bird_data: DoorBirdData,
        url: str,
        camera_id: str,
        interval: dt.timedelta,
        stream_url: str | None = None,
        *,
        enabled_default: bool = True,
    ) -> None:
        """Initialize the camera on a DoorBird device."""
        super().__init__(door_bird_data)
        self._attr_entity_registry_enabled_default = enabled_default
        self._url = url
        self._stream_url = stream_url
        self._attr_translation_key = camera_id
        self._last_image: bytes | None = None
        if self._stream_url:
            self._attr_supported_features = CameraEntityFeature.STREAM
            self._attr_camera_entity_capabilities = CameraEntityCapabilities(
                two_way_audio=True,
                # The RTSP stream has no audio
                stream_sources=(
                    CameraStreamSource(key="video", media=CameraStreamMedia.VIDEO),
                    CameraStreamSource(key="audio", media=CameraStreamMedia.AUDIO),
                ),
            )
        self._interval = interval
        self._last_update = dt.datetime.min
        self._attr_unique_id = f"{self._mac_addr}_{camera_id}"

    @override
    async def async_stream_source_url(self, key: str) -> str | None:
        """Return the URL of a stream source."""
        if key == "audio":
            return self._door_station.device.live_audio_url
        return self._stream_url

    @override
    async def async_send_backchannel_audio(self, audio: AsyncIterator[bytes]) -> None:
        """Send talk-back audio to the speaker.

        The device takes it as one HTTP/1.0 request with a made-up length,
        and only answers when it refuses the audio.
        """
        url = URL(self._door_station.device.live_audio_transmit_url)
        authorization = encode_basic_auth(url.user or "", url.password or "")
        try:
            reader, writer = await asyncio.open_connection(
                url.host, url.port, ssl=url.scheme == "https"
            )
        except OSError as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN, translation_key="talk_back_failed"
            ) from err
        try:
            writer.write(
                (
                    f"POST {url.raw_path} HTTP/1.0\r\n"
                    f"Authorization: {authorization}\r\n"
                    "Content-Type: audio/basic\r\n"
                    "Content-Length: 9999999\r\n"
                    "Connection: Keep-Alive\r\n"
                    "Cache-Control: no-cache\r\n\r\n"
                ).encode()
            )
            with suppress(TimeoutError):
                async with asyncio.timeout(_TALK_BACK_REFUSAL_TIMEOUT):
                    status = (await reader.readline()).split()
                if len(status) < 2 or status[1] != b"200":
                    raise HomeAssistantError(
                        translation_domain=DOMAIN, translation_key="talk_back_refused"
                    )
            async for chunk in audio:
                writer.write(chunk)
                await writer.drain()
        except OSError as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN, translation_key="talk_back_failed"
            ) from err
        finally:
            writer.close()

    @override
    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Pull a still image from the camera."""
        now = dt_util.utcnow()

        if self._last_image and now - self._last_update < self._interval:
            return self._last_image

        try:
            self._last_image = await self._door_station.device.get_image(
                self._url, timeout=_TIMEOUT
            )
        # pylint: disable-next=home-assistant-action-swallowed-exception
        except TimeoutError:
            _LOGGER.error("DoorBird %s: Camera image timed out", self.name)
            return self._last_image
        except aiohttp.ClientError as error:
            _LOGGER.error(
                "DoorBird %s: Error getting camera image: %s", self.name, error
            )
            return self._last_image

        self._last_update = now
        return self._last_image
