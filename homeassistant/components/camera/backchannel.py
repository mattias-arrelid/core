"""Local RTSP endpoint for cameras that send the backchannel audio themselves.

A WebRTC provider sends the client's audio to a camera over the ONVIF
backchannel of an RTSP stream. For a camera whose talk-back needs a vendor
API, this endpoint is that stream: it only has a backchannel track, and
hands what it receives to the camera entity.
"""

import asyncio
from collections.abc import AsyncIterator
import logging
import secrets
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util.hass_dict import HassKey

from .const import CameraBackchannelCodec

if TYPE_CHECKING:
    from .entity import Camera

_LOGGER = logging.getLogger(__name__)

DATA_BACKCHANNEL_SERVER: HassKey[BackchannelServer] = HassKey(
    "camera_backchannel_server"
)

_HOST = "127.0.0.1"
_RTP_HEADER_SIZE = 12
_MAX_QUEUED_PACKETS = 50
# A client that stops talking stops sending audio, which frees the device
_IDLE_TIMEOUT = 1.0
# RTP payload type and format of each codec (RFC 3551, RFC 7587)
_RTP_FORMATS = {
    CameraBackchannelCodec.PCMU: (0, "PCMU/8000"),
    CameraBackchannelCodec.PCMA: (8, "PCMA/8000"),
    CameraBackchannelCodec.OPUS: (96, "opus/48000/2"),
}


def _sdp(codec: CameraBackchannelCodec) -> str:
    """Return the description of a stream with one backchannel track."""
    payload_type, rtp_format = _RTP_FORMATS[codec]
    return "\r\n".join(
        (
            "v=0",
            f"o=- 0 0 IN IP4 {_HOST}",
            "s=backchannel",
            "t=0 0",
            f"m=audio 0 RTP/AVP {payload_type}",
            f"a=rtpmap:{payload_type} {rtp_format}",
            "a=sendonly",
            "a=control:track0",
            "",
        )
    )


def _rtp_payload(packet: bytes) -> bytes:
    """Return the payload of an RTP packet (RFC 3550 section 5.1)."""
    if len(packet) < _RTP_HEADER_SIZE:
        return b""
    offset = _RTP_HEADER_SIZE + 4 * (packet[0] & 0x0F)
    if packet[0] & 0x10 and len(packet) >= offset + 4:
        offset += 4 + 4 * int.from_bytes(packet[offset + 2 : offset + 4], "big")
    end = len(packet)
    if packet[0] & 0x20 and end > offset:
        end -= packet[-1]
    return packet[offset:end]


def _end_audio(queue: asyncio.Queue[bytes | None]) -> None:
    """Tell the camera that the audio has ended."""
    if queue.full():
        queue.get_nowait()
    queue.put_nowait(None)


class BackchannelServer:
    """RTSP server with one backchannel-only stream per camera."""

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the server."""
        self._hass = hass
        self._server: asyncio.Server | None = None
        self._start_lock = asyncio.Lock()
        self._cameras: dict[str, Camera] = {}
        self._tokens: dict[Camera, str] = {}
        self._talking: set[Camera] = set()

    async def async_get_url(self, camera: Camera) -> str:
        """Return the URL a provider sends the camera's backchannel audio to."""
        async with self._start_lock:
            if self._server is None:
                self._server = await asyncio.start_server(self._handle, _HOST, 0)
                self._hass.bus.async_listen_once(
                    EVENT_HOMEASSISTANT_STOP, self._async_stop
                )
        if (token := self._tokens.get(camera)) is None:
            token = self._tokens[camera] = secrets.token_hex(16)
            self._cameras[token] = camera
        port = self._server.sockets[0].getsockname()[1]
        return f"rtsp://{_HOST}:{port}/{token}"

    def async_remove(self, camera: Camera) -> None:
        """Forget a camera."""
        if token := self._tokens.pop(camera, None):
            del self._cameras[token]

    async def _async_stop(self, event: Event) -> None:
        """Stop the server."""
        if self._server:
            self._server.close()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Serve one RTSP connection."""
        camera: Camera | None = None
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(_MAX_QUEUED_PACKETS)
        sender: asyncio.Task[None] | None = None
        last_audio = 0.0
        try:
            while True:
                try:
                    async with asyncio.timeout_at(
                        last_audio + _IDLE_TIMEOUT if sender else None
                    ):
                        first = await reader.readexactly(1)
                except TimeoutError:
                    _end_audio(queue)
                    queue = asyncio.Queue(_MAX_QUEUED_PACKETS)
                    sender = None
                    continue
                if first == b"$":
                    header = await reader.readexactly(3)
                    packet = await reader.readexactly(int.from_bytes(header[1:], "big"))
                    # Channel 0 is RTP, channel 1 is RTCP
                    if camera is None or header[0] != 0:
                        continue
                    if not (payload := _rtp_payload(packet)):
                        continue
                    last_audio = self._hass.loop.time()
                    if sender is None:
                        # The first client to talk keeps the device until it stops
                        if camera in self._talking:
                            continue
                        self._talking.add(camera)
                        sender = self._hass.async_create_background_task(
                            self._async_send(camera, queue),
                            f"camera backchannel {camera.entity_id}",
                        )
                    elif sender.done():
                        break
                    if queue.full():
                        queue.get_nowait()
                    queue.put_nowait(payload)
                    continue

                head = (first + await reader.readuntil(b"\r\n\r\n")).decode()
                request, *lines = head.split("\r\n")
                method, url = request.split(" ")[:2]
                headers = {
                    key.lower(): value
                    for key, _, value in (line.partition(": ") for line in lines)
                }
                token = urlsplit(url).path.strip("/").split("/")[0]
                status = "200 OK"
                response = [f"CSeq: {headers.get('cseq', '0')}"]
                body = ""
                if method == "OPTIONS":
                    response.append("Public: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN")
                elif (camera := self._cameras.get(token)) is None:
                    status = "404 Not Found"
                elif method == "DESCRIBE":
                    body = _sdp(camera.backchannel_codec)
                    response += [
                        "Content-Type: application/sdp",
                        f"Content-Base: {url}/",
                        f"Content-Length: {len(body)}",
                    ]
                elif method == "SETUP":
                    if "interleaved" not in (transport := headers.get("transport", "")):
                        status = "461 Unsupported Transport"
                    else:
                        response += [f"Transport: {transport}", f"Session: {token}"]
                else:
                    response.append(f"Session: {token}")
                writer.write(
                    (
                        f"RTSP/1.0 {status}\r\n"
                        + "\r\n".join(response)
                        + "\r\n\r\n"
                        + body
                    ).encode()
                )
                await writer.drain()
                if method == "TEARDOWN":
                    break
        except asyncio.IncompleteReadError, ConnectionError:
            pass
        finally:
            if sender:
                _end_audio(queue)
            writer.close()

    async def _async_send(
        self, camera: Camera, queue: asyncio.Queue[bytes | None]
    ) -> None:
        """Hand the received audio to the camera."""

        async def audio() -> AsyncIterator[bytes]:
            while (chunk := await queue.get()) is not None:
                yield chunk

        try:
            await camera.async_send_backchannel_audio(audio())
        except HomeAssistantError as err:
            _LOGGER.warning(
                "Error sending backchannel audio for %s: %s", camera.entity_id, err
            )
        finally:
            self._talking.discard(camera)


def async_get_backchannel_server(hass: HomeAssistant) -> BackchannelServer:
    """Return the backchannel server."""
    if (server := hass.data.get(DATA_BACKCHANNEL_SERVER)) is None:
        server = hass.data[DATA_BACKCHANNEL_SERVER] = BackchannelServer(hass)
    return server
