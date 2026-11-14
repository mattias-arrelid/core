"""Test the local endpoint for backchannel audio."""

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import patch
from urllib.parse import urlsplit

import pytest

from homeassistant.components.camera import (
    Camera,
    CameraBackchannelCodec,
    CameraEntityCapabilities,
    CameraStreamMedia,
)
from homeassistant.components.camera.backchannel import async_get_backchannel_server
from homeassistant.core import HomeAssistant

_RTP_HEADER = bytes([0x80, 0x00, 0x00, 0x01]) + bytes(8)


class _TalkingCamera(Camera):
    """A camera that sends the backchannel audio itself."""

    def __init__(self, hass: HomeAssistant, *, two_way_audio: bool = True) -> None:
        """Initialize the camera."""
        super().__init__()
        self.hass = hass
        self.entity_id = "camera.talking"
        self._attr_camera_entity_capabilities = CameraEntityCapabilities(
            two_way_audio=two_way_audio
        )
        self.received: list[bytes] = []
        self.calls = 0
        self.done = asyncio.Event()

    async def stream_source(self) -> str | None:
        """Return the source of the stream."""
        return "rtsp://camera/main"

    async def async_send_backchannel_audio(self, audio: AsyncIterator[bytes]) -> None:
        """Collect the audio."""
        self.received.extend([chunk async for chunk in audio])
        self.calls += 1
        self.done.set()


async def _request(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    method: str,
    url: str,
    extra: str = "",
) -> str:
    """Send an RTSP request and return the response."""
    writer.write(f"{method} {url} RTSP/1.0\r\nCSeq: 1\r\n{extra}\r\n".encode())
    await writer.drain()
    head = (await reader.readuntil(b"\r\n\r\n")).decode()
    for line in head.split("\r\n"):
        if line.startswith("Content-Length: "):
            head += (await reader.readexactly(int(line.split(": ")[1]))).decode()
    return head


@pytest.mark.usefixtures("socket_enabled")
@pytest.mark.parametrize(
    ("codec", "expected_description"),
    [
        pytest.param(
            CameraBackchannelCodec.PCMU,
            "m=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\na=sendonly",
            id="pcmu",
        ),
        pytest.param(
            CameraBackchannelCodec.PCMA,
            "m=audio 0 RTP/AVP 8\r\na=rtpmap:8 PCMA/8000\r\na=sendonly",
            id="pcma",
        ),
        pytest.param(
            CameraBackchannelCodec.OPUS,
            "m=audio 0 RTP/AVP 96\r\na=rtpmap:96 opus/48000/2\r\na=sendonly",
            id="opus",
        ),
    ],
)
async def test_backchannel_audio_reaches_camera(
    hass: HomeAssistant, codec: CameraBackchannelCodec, expected_description: str
) -> None:
    """Test audio sent over the RTSP backchannel is handed to the camera."""
    camera = _TalkingCamera(hass)
    camera._attr_backchannel_codec = codec
    url = await async_get_backchannel_server(hass).async_get_url(camera)
    assert await async_get_backchannel_server(hass).async_get_url(camera) == url

    reader, writer = await asyncio.open_connection("127.0.0.1", urlsplit(url).port)
    describe = await _request(
        reader, writer, "DESCRIBE", url, "Require: www.onvif.org/ver20/backchannel\r\n"
    )
    assert describe.startswith("RTSP/1.0 200 OK")
    assert expected_description in describe
    setup = await _request(
        reader,
        writer,
        "SETUP",
        f"{url}/track0",
        "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n",
    )
    assert "Transport: RTP/AVP/TCP;unicast;interleaved=0-1" in setup
    assert (await _request(reader, writer, "PLAY", url)).startswith("RTSP/1.0 200 OK")

    for payload in (b"one", b"two"):
        packet = _RTP_HEADER + payload
        writer.write(b"$\x00" + len(packet).to_bytes(2, "big") + packet)
    # RTCP on channel 1 is ignored
    writer.write(b"$\x01\x00\x04rtcp")
    await _request(reader, writer, "TEARDOWN", url)
    writer.close()

    async with asyncio.timeout(5):
        await camera.done.wait()
    assert camera.received == [b"one", b"two"]


@pytest.mark.usefixtures("socket_enabled")
async def test_backchannel_pause_ends_audio(hass: HomeAssistant) -> None:
    """Test the camera is let go when the audio pauses, and called again after."""
    camera = _TalkingCamera(hass)
    url = await async_get_backchannel_server(hass).async_get_url(camera)

    reader, writer = await asyncio.open_connection("127.0.0.1", urlsplit(url).port)
    await _request(reader, writer, "DESCRIBE", url)
    await _request(reader, writer, "PLAY", url)
    packet = _RTP_HEADER + b"one"
    with patch("homeassistant.components.camera.backchannel._IDLE_TIMEOUT", 0.01):
        writer.write(b"$\x00" + len(packet).to_bytes(2, "big") + packet)
        async with asyncio.timeout(5):
            await camera.done.wait()
        assert camera.calls == 1

        camera.done.clear()
        writer.write(b"$\x00" + len(packet).to_bytes(2, "big") + packet)
        async with asyncio.timeout(5):
            await camera.done.wait()
    writer.close()

    assert camera.calls == 2
    assert camera.received == [b"one", b"one"]


@pytest.mark.usefixtures("socket_enabled")
async def test_backchannel_one_talker_at_a_time(hass: HomeAssistant) -> None:
    """Test a second client's audio is dropped while the first one talks."""
    camera = _TalkingCamera(hass)
    url = await async_get_backchannel_server(hass).async_get_url(camera)
    port = urlsplit(url).port

    first = await asyncio.open_connection("127.0.0.1", port)
    second = await asyncio.open_connection("127.0.0.1", port)
    for reader, writer in (first, second):
        await _request(reader, writer, "DESCRIBE", url)
        await _request(reader, writer, "PLAY", url)
    for (reader, writer), payload in ((first, b"first"), (second, b"second")):
        packet = _RTP_HEADER + payload
        writer.write(b"$\x00" + len(packet).to_bytes(2, "big") + packet)
        # A request after the audio, so the audio has been read
        await _request(reader, writer, "OPTIONS", url)
    for reader, writer in (second, first):
        await _request(reader, writer, "TEARDOWN", url)
        writer.close()

    async with asyncio.timeout(5):
        await camera.done.wait()
    assert camera.calls == 1
    assert camera.received == [b"first"]


@pytest.mark.usefixtures("socket_enabled")
async def test_backchannel_unknown_stream(hass: HomeAssistant) -> None:
    """Test a stream that belongs to no camera is not found."""
    server = async_get_backchannel_server(hass)
    camera = _TalkingCamera(hass)
    url = await server.async_get_url(camera)
    server.async_remove(camera)

    reader, writer = await asyncio.open_connection("127.0.0.1", urlsplit(url).port)
    response = await _request(reader, writer, "DESCRIBE", url)
    writer.close()

    assert response.startswith("RTSP/1.0 404 Not Found")


@pytest.mark.usefixtures("socket_enabled")
async def test_stream_sources_with_own_backchannel(hass: HomeAssistant) -> None:
    """Test a camera that sends the audio itself gets the local endpoint."""
    camera = _TalkingCamera(hass)

    sources = await camera.async_get_stream_sources()

    url = await async_get_backchannel_server(hass).async_get_url(camera)
    assert sources == {
        "rtsp://camera/main": CameraStreamMedia.VIDEO | CameraStreamMedia.AUDIO,
        url: CameraStreamMedia.BACKCHANNEL,
    }
    assert await camera.async_get_stream_sources(CameraStreamMedia.VIDEO) == {
        "rtsp://camera/main": CameraStreamMedia.VIDEO
    }


async def test_stream_sources_without_two_way_audio(hass: HomeAssistant) -> None:
    """Test the local endpoint is only given with two-way audio."""
    camera = _TalkingCamera(hass, two_way_audio=False)

    assert await camera.async_get_stream_sources() == {
        "rtsp://camera/main": CameraStreamMedia.VIDEO | CameraStreamMedia.AUDIO
    }
