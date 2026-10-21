"""Action chords and modifiers must agree across event and resync paths."""
import asyncio
import io
from types import SimpleNamespace

import evdev.ecodes as e
import pytest

from evpipe import hid_map, wire
from evpipe.send import SenderApp, Source


@pytest.mark.parametrize("local_only", [[], [e.KEY_LEFTMETA]])
def test_resync_excludes_consumed_action_trigger(local_only):
    out = io.BytesIO()
    app = SenderApp([], [], out, local_only_evdev=local_only)
    src = Source(
        path="fake", role=wire.DEV_KB, device_id=0,
        dev=SimpleNamespace(active_keys=lambda: [e.KEY_LEFTMETA, e.KEY_D]),
        descriptor=wire.DeviceDescriptor(name="kb", vendor_id=0, product_id=0, kind=wire.DEV_KB),
        grabbed=True, swallow_release={e.KEY_D},
    )
    asyncio.run(app._emit_full_state(src))
    out.seek(0)
    kind, payload = wire.read_packet(out)
    assert kind == wire.PACKET_FULL_STATE
    _, held = wire.decode_full_state(payload)
    assert held == ([] if local_only else [hid_map.encode_key(e.KEY_LEFTMETA)])
