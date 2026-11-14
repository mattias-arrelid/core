"""Test DoorBird cameras."""

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import patch

from freezegun.api import FrozenDateTimeFactory
import pytest
from pytest_homeassistant.common import MockConfigEntry

from homeassistant.components.automation import DOMAIN as AUTOMATION_DOMAIN
from homeassistant.components.camera import (
    CameraState,
    CameraStreamMedia,
    async_get_image,
    async_get_stream_source,
)
from homeassistant.components.camera.backchannel import async_get_backchannel_server
from homeassistant.components.camera.helper import get_camera_from_entity_id
from homeassistant.components.doorbird.const import (
    CONF_EVENTS,
    DEFAULT_DOORBELL_EVENT,
    DEFAULT_MOTION_EVENT,
    DOMAIN,
)
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import (
    device_registry as dr,
    entity_registry as er,
    issue_registry as ir,
)
from homeassistant.setup import async_setup_component

from . import VALID_CONFIG, mock_not_found_exception
from .conftest import DoorbirdMockerType

LIVE_CAMERA_ENTITY_ID = "camera.mydoorbird_live"
LIVE_1080P_CAMERA_ENTITY_ID = "camera.mydoorbird_live_1080p"
LAST_RING_CAMERA_ENTITY_ID = "camera.mydoorbird_last_ring"
LAST_MOTION_CAMERA_ENTITY_ID = "camera.mydoorbird_last_motion"
LAST_RING_ISSUE_ID = "deprecated_camera_1234ABCD_last_ring"
_TRANSMIT_URL = "http://user:pass@127.0.0.1/bha-api/audio-transmit.cgi"


def create_stale_issue(hass: HomeAssistant, issue_id: str) -> None:
    """Create a repair issue left over from an earlier setup."""
    ir.async_create_issue(
        hass,
        DOMAIN,
        issue_id,
        is_fixable=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key="deprecated_camera_last_ring",
        translation_placeholders={
            "entity_id": LAST_RING_CAMERA_ENTITY_ID,
            "entity_name": "Last ring",
            "docs_url": "",
        },
    )


async def test_doorbird_live_camera(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Test the live camera, which has no image entity replacement."""
    doorbird_entry = await doorbird_mocker()
    assert hass.states.get(LIVE_CAMERA_ENTITY_ID).state == CameraState.IDLE
    assert await async_get_stream_source(hass, LIVE_CAMERA_ENTITY_ID) is not None
    api = doorbird_entry.api
    api.get_image.side_effect = mock_not_found_exception()
    with pytest.raises(HomeAssistantError):
        await async_get_image(hass, LIVE_CAMERA_ENTITY_ID)
    api.get_image.side_effect = TimeoutError()
    with pytest.raises(HomeAssistantError):
        await async_get_image(hass, LIVE_CAMERA_ENTITY_ID)
    api.get_image.side_effect = None
    assert (await async_get_image(hass, LIVE_CAMERA_ENTITY_ID)).content == b"image"
    api.get_image.return_value = b"notyet"
    # Ensure rate limit works
    assert (await async_get_image(hass, LIVE_CAMERA_ENTITY_ID)).content == b"image"

    freezer.tick(60)
    assert (await async_get_image(hass, LIVE_CAMERA_ENTITY_ID)).content == b"notyet"


async def test_live_1080p_camera_disabled_by_default(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    entity_registry: er.EntityRegistry,
) -> None:
    """Test the 1080p camera is disabled by default."""
    await doorbird_mocker()

    assert hass.states.get(LIVE_1080P_CAMERA_ENTITY_ID) is None
    entry = entity_registry.async_get(LIVE_1080P_CAMERA_ENTITY_ID)
    assert entry is not None
    assert entry.disabled_by is er.RegistryEntryDisabler.INTEGRATION


@pytest.mark.usefixtures("socket_enabled")
async def test_live_camera_stream_sources(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
) -> None:
    """Test the live camera lists the audio the RTSP stream lacks."""
    doorbird_entry = await doorbird_mocker()

    camera = get_camera_from_entity_id(hass, LIVE_CAMERA_ENTITY_ID)
    sources = await camera.async_get_stream_sources()
    backchannel_url = await async_get_backchannel_server(hass).async_get_url(camera)
    assert sources == {
        doorbird_entry.api.rtsp_live_video_url: CameraStreamMedia.VIDEO,
        doorbird_entry.api.live_audio_url: CameraStreamMedia.AUDIO,
        backchannel_url: CameraStreamMedia.BACKCHANNEL,
    }


async def _audio() -> AsyncIterator[bytes]:
    yield b"one"
    yield b"two"


@pytest.mark.parametrize(
    ("answer", "expected_error", "expected_audio"),
    [
        pytest.param(b"", None, b"onetwo", id="accepted_without_answer"),
        pytest.param(b"HTTP/1.0 200 OK\r\n\r\n", None, b"onetwo", id="accepted"),
        pytest.param(
            b"HTTP/1.0 204 No Content\r\n\r\n",
            "talk_back_refused",
            b"",
            id="refused",
        ),
    ],
)
@pytest.mark.usefixtures("socket_enabled")
async def test_live_camera_sends_backchannel_audio(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    answer: bytes,
    expected_error: str | None,
    expected_audio: bytes,
) -> None:
    """Test talk-back audio is posted to the speaker the way the device wants it."""
    received = bytearray()
    done = asyncio.Event()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        received.extend(await reader.readuntil(b"\r\n\r\n"))
        writer.write(answer)
        await writer.drain()
        received.extend(await reader.read())
        writer.close()
        done.set()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    doorbird_entry = await doorbird_mocker()
    doorbird_entry.api.live_audio_transmit_url = (
        f"http://user:pass@127.0.0.1:{port}/bha-api/audio-transmit.cgi"
    )
    camera = get_camera_from_entity_id(hass, LIVE_CAMERA_ENTITY_ID)

    error: str | None = None
    try:
        await camera.async_send_backchannel_audio(_audio())
    except HomeAssistantError as err:
        error = err.translation_key
    async with asyncio.timeout(5):
        await done.wait()
    server.close()

    assert error == expected_error
    head, _, audio = bytes(received).partition(b"\r\n\r\n")
    assert head.split(b"\r\n") == [
        b"POST /bha-api/audio-transmit.cgi HTTP/1.0",
        b"Authorization: Basic dXNlcjpwYXNz",
        b"Content-Type: audio/basic",
        b"Content-Length: 9999999",
        b"Connection: Keep-Alive",
        b"Cache-Control: no-cache",
    ]
    assert audio == expected_audio


async def test_live_camera_backchannel_audio_unreachable(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
) -> None:
    """Test a speaker that can't be reached raises."""
    doorbird_entry = await doorbird_mocker()
    doorbird_entry.api.live_audio_transmit_url = _TRANSMIT_URL
    camera = get_camera_from_entity_id(hass, LIVE_CAMERA_ENTITY_ID)

    with (
        patch("asyncio.open_connection", side_effect=OSError),
        pytest.raises(HomeAssistantError),
    ):
        await camera.async_send_backchannel_audio(_audio())


@pytest.mark.usefixtures("entity_registry_enabled_by_default")
async def test_live_1080p_camera(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
) -> None:
    """Test the 1080p camera streams from the 1080p RTSP URL."""
    doorbird_entry = await doorbird_mocker()

    assert hass.states.get(LIVE_1080P_CAMERA_ENTITY_ID).state == CameraState.IDLE
    assert (
        await async_get_stream_source(hass, LIVE_1080P_CAMERA_ENTITY_ID)
        == doorbird_entry.api.rtsp_live_video_1080p_url
    )


@pytest.mark.parametrize(
    ("camera_id", "entity_id"),
    [
        pytest.param("last_ring", LAST_RING_CAMERA_ENTITY_ID, id="last_ring"),
        pytest.param("last_motion", LAST_MOTION_CAMERA_ENTITY_ID, id="last_motion"),
    ],
)
async def test_deprecated_camera_not_created_for_new_installs(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    entity_registry: er.EntityRegistry,
    issue_registry: ir.IssueRegistry,
    camera_id: str,
    entity_id: str,
) -> None:
    """Test the deprecated still cameras are not created on a fresh install."""
    create_stale_issue(hass, f"deprecated_camera_1234ABCD_{camera_id}")

    await doorbird_mocker()

    assert hass.states.get(entity_id) is None
    assert (
        entity_registry.async_get_entity_id(
            Platform.CAMERA, DOMAIN, f"1234ABCD_{camera_id}"
        )
        is None
    )
    assert (
        DOMAIN,
        f"deprecated_camera_1234ABCD_{camera_id}",
    ) not in issue_registry.issues


@pytest.mark.parametrize(
    ("camera_id", "entity_id"),
    [
        pytest.param("last_ring", LAST_RING_CAMERA_ENTITY_ID, id="last_ring"),
        pytest.param("last_motion", LAST_MOTION_CAMERA_ENTITY_ID, id="last_motion"),
    ],
)
async def test_deprecated_camera_kept_and_flagged(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    entity_registry: er.EntityRegistry,
    issue_registry: ir.IssueRegistry,
    camera_id: str,
    entity_id: str,
) -> None:
    """Test an existing still camera is kept and raises a repair issue."""
    entity_registry.async_get_or_create(
        Platform.CAMERA,
        DOMAIN,
        f"1234ABCD_{camera_id}",
        suggested_object_id=f"mydoorbird_{camera_id}",
    )

    doorbird_entry = await doorbird_mocker()

    assert hass.states.get(entity_id).state == CameraState.IDLE
    issue = issue_registry.async_get_issue(
        DOMAIN, f"deprecated_camera_1234ABCD_{camera_id}"
    )
    assert issue is not None
    assert issue.translation_key == f"deprecated_camera_{camera_id}"
    assert issue.translation_placeholders["entity_id"] == entity_id
    assert issue.config_entry_id == doorbird_entry.entry.entry_id


async def test_deprecated_camera_issue_names_renamed_entity(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    entity_registry: er.EntityRegistry,
    issue_registry: ir.IssueRegistry,
) -> None:
    """Test the issue names the entity id the user gave the camera."""
    entity_registry.async_get_or_create(
        Platform.CAMERA,
        DOMAIN,
        "1234ABCD_last_ring",
        suggested_object_id="front_door_ring_snapshot",
    )

    await doorbird_mocker()

    issue = issue_registry.async_get_issue(DOMAIN, LAST_RING_ISSUE_ID)
    assert issue is not None
    assert issue.translation_placeholders["entity_id"] == (
        "camera.front_door_ring_snapshot"
    )


@pytest.mark.parametrize(
    "camera_id",
    [
        pytest.param("last_ring", id="last_ring"),
        pytest.param("last_motion", id="last_motion"),
    ],
)
async def test_deprecated_camera_removed_when_disabled(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    entity_registry: er.EntityRegistry,
    issue_registry: ir.IssueRegistry,
    camera_id: str,
) -> None:
    """Test a disabled still camera is removed and the repair issue cleared."""
    entity_registry.async_get_or_create(
        Platform.CAMERA,
        DOMAIN,
        f"1234ABCD_{camera_id}",
        suggested_object_id=f"mydoorbird_{camera_id}",
        disabled_by=er.RegistryEntryDisabler.USER,
    )
    create_stale_issue(hass, f"deprecated_camera_1234ABCD_{camera_id}")

    await doorbird_mocker()

    assert (
        entity_registry.async_get_entity_id(
            Platform.CAMERA, DOMAIN, f"1234ABCD_{camera_id}"
        )
        is None
    )
    assert (
        DOMAIN,
        f"deprecated_camera_1234ABCD_{camera_id}",
    ) not in issue_registry.issues


async def test_deprecated_camera_kept_when_disabled_by_device(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
    issue_registry: ir.IssueRegistry,
) -> None:
    """Test a camera disabled along with its device is neither removed nor flagged."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="1CCAE3AAAAAA",
        data=VALID_CONFIG,
        options={CONF_EVENTS: [DEFAULT_DOORBELL_EVENT, DEFAULT_MOTION_EVENT]},
    )
    entry.add_to_hass(hass)
    device_entry = device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        connections={(dr.CONNECTION_NETWORK_MAC, "1234ABCD")},
        disabled_by=dr.DeviceEntryDisabler.USER,
    )
    entity_registry.async_get_or_create(
        Platform.CAMERA,
        DOMAIN,
        "1234ABCD_last_ring",
        config_entry=entry,
        device_id=device_entry.id,
        suggested_object_id="mydoorbird_last_ring",
    )
    create_stale_issue(hass, LAST_RING_ISSUE_ID)

    await doorbird_mocker(entry=entry)

    assert entity_registry.async_get(LAST_RING_CAMERA_ENTITY_ID) is not None
    assert (DOMAIN, LAST_RING_ISSUE_ID) not in issue_registry.issues


@pytest.mark.parametrize(
    ("automation_id", "expected_item"),
    [
        pytest.param(
            {"id": "test_automation"},
            "[test_automation](/config/automation/edit/test_automation)",
            id="registered",
        ),
        pytest.param({}, "`automation.test_automation`", id="yaml_without_id"),
    ],
)
async def test_deprecated_camera_issue_lists_automations(
    hass: HomeAssistant,
    doorbird_mocker: DoorbirdMockerType,
    entity_registry: er.EntityRegistry,
    issue_registry: ir.IssueRegistry,
    automation_id: dict[str, str],
    expected_item: str,
) -> None:
    """Test the issue lists the automations still using the camera."""
    entity_registry.async_get_or_create(
        Platform.CAMERA,
        DOMAIN,
        "1234ABCD_last_ring",
        suggested_object_id="mydoorbird_last_ring",
    )
    assert await async_setup_component(
        hass,
        AUTOMATION_DOMAIN,
        {
            AUTOMATION_DOMAIN: {
                **automation_id,
                "alias": "test_automation",
                "triggers": {
                    "trigger": "state",
                    "entity_id": LAST_RING_CAMERA_ENTITY_ID,
                },
                "actions": {"action": "notify.notify", "data": {}},
            }
        },
    )

    await doorbird_mocker()

    issue = issue_registry.async_get_issue(DOMAIN, LAST_RING_ISSUE_ID)
    assert issue is not None
    assert issue.translation_key == "deprecated_camera_last_ring_scripts"
    assert issue.translation_placeholders["items"] == f"- {expected_item}"
