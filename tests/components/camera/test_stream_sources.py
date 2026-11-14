"""Test the stream sources a camera lists."""

import pytest

from homeassistant.components.camera import (
    Camera,
    CameraEntityCapabilities,
    CameraStreamMedia,
    CameraStreamSource,
)

_VIDEO = CameraStreamMedia.VIDEO
_AUDIO = CameraStreamMedia.AUDIO
_BACKCHANNEL = CameraStreamMedia.BACKCHANNEL
_ALL = _VIDEO | _AUDIO | _BACKCHANNEL
_URLS = {
    "main": "rtsp://camera/main",
    "audio": "http://camera/audio",
    "talk": "rtsp://camera/talk",
    "same_as_main": "rtsp://camera/main",
    "missing": None,
}


class _Camera(Camera):
    """A camera that lists its stream sources."""

    async def async_stream_source_url(self, key: str) -> str | None:
        """Return the URL of a listed stream source."""
        return _URLS[key]


class _SingleSourceCamera(Camera):
    """A camera with only a stream source."""

    async def stream_source(self) -> str | None:
        """Return the source of the stream."""
        return "rtsp://camera/main"


@pytest.mark.parametrize(
    ("stream_sources", "two_way_audio", "media", "expected"),
    [
        pytest.param(
            (
                CameraStreamSource(key="main", media=_VIDEO),
                CameraStreamSource(key="audio", media=_AUDIO),
            ),
            False,
            _ALL,
            {"rtsp://camera/main": _VIDEO, "http://camera/audio": _AUDIO},
            id="separate_audio",
        ),
        pytest.param(
            (
                CameraStreamSource(key="main"),
                CameraStreamSource(key="talk", media=_BACKCHANNEL),
            ),
            True,
            _ALL,
            {"rtsp://camera/main": _VIDEO | _AUDIO, "rtsp://camera/talk": _BACKCHANNEL},
            id="separate_backchannel",
        ),
        pytest.param(
            (
                CameraStreamSource(key="main"),
                CameraStreamSource(key="talk", media=_BACKCHANNEL),
            ),
            False,
            _ALL,
            {"rtsp://camera/main": _VIDEO | _AUDIO},
            id="no_backchannel_without_two_way_audio",
        ),
        pytest.param(
            (
                CameraStreamSource(key="main"),
                CameraStreamSource(key="talk", media=_BACKCHANNEL),
            ),
            True,
            _VIDEO,
            {"rtsp://camera/main": _VIDEO},
            id="only_the_media_asked_for",
        ),
        pytest.param(
            (
                CameraStreamSource(key="main"),
                CameraStreamSource(key="audio", media=_AUDIO),
            ),
            False,
            _ALL,
            {"rtsp://camera/main": _VIDEO | _AUDIO},
            id="first_source_with_a_kind_wins",
        ),
        pytest.param(
            (
                CameraStreamSource(key="main", media=_VIDEO),
                CameraStreamSource(key="audio", media=_AUDIO),
            ),
            True,
            _ALL,
            {"rtsp://camera/main": _VIDEO, "http://camera/audio": _AUDIO},
            id="media_no_source_has_left_out",
        ),
        pytest.param(
            (
                CameraStreamSource(key="main", media=_VIDEO),
                CameraStreamSource(key="missing", media=_AUDIO),
                CameraStreamSource(key="audio", media=_AUDIO),
            ),
            False,
            _ALL,
            {"rtsp://camera/main": _VIDEO, "http://camera/audio": _AUDIO},
            id="source_without_url_skipped",
        ),
        pytest.param(
            (
                CameraStreamSource(key="main", media=_VIDEO),
                CameraStreamSource(key="same_as_main", media=_AUDIO),
            ),
            False,
            _ALL,
            {"rtsp://camera/main": _VIDEO | _AUDIO},
            id="same_url_merged",
        ),
    ],
)
async def test_listed_stream_sources(
    stream_sources: tuple[CameraStreamSource, ...],
    two_way_audio: bool,
    media: CameraStreamMedia,
    expected: dict[str, CameraStreamMedia],
) -> None:
    """Test each kind of media comes from the first listed source that has it."""
    camera = _Camera()
    camera._attr_stream_sources = stream_sources
    camera._attr_camera_entity_capabilities = CameraEntityCapabilities(
        two_way_audio=two_way_audio
    )

    assert await camera.async_get_stream_sources(media) == expected
    assert await camera.stream_source() == "rtsp://camera/main"


@pytest.mark.parametrize(
    ("two_way_audio", "expected"),
    [
        pytest.param(False, {"rtsp://camera/main": _VIDEO | _AUDIO}, id="one_way"),
        pytest.param(True, {"rtsp://camera/main": _ALL}, id="two_way_audio"),
    ],
)
async def test_single_stream_source(
    two_way_audio: bool, expected: dict[str, CameraStreamMedia]
) -> None:
    """Test a camera that lists nothing has its stream source for everything."""
    camera = _SingleSourceCamera()
    camera._attr_camera_entity_capabilities = CameraEntityCapabilities(
        two_way_audio=two_way_audio
    )

    assert await camera.async_get_stream_sources() == expected


async def test_no_stream_source() -> None:
    """Test a camera without a stream source has no sources."""
    assert await Camera().async_get_stream_sources() == {}
