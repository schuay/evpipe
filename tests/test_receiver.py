"""Receiver lifecycle checks without real input devices."""
import asyncio

import pytest

from evpipe import recv, wire


@pytest.mark.parametrize("stop", ["signal", "timeout", "eof"])
def test_receiver_exits_without_waiting_for_more_input(monkeypatch, stop):
    async def run():
        app = recv.ReceiverApp()
        reader = asyncio.StreamReader()
        reader.feed_data(wire.encode_session_open([]))
        if stop == "eof":
            reader.feed_eof()
        elif stop == "signal":
            asyncio.get_running_loop().call_later(0.01, app.shutdown)
        assert await asyncio.wait_for(app.run(reader), 0.5) == 0

    monkeypatch.setattr(recv, "HEARTBEAT_DEAD_S", 0.02)
    monkeypatch.setattr(recv, "HEARTBEAT_CHECK_S", 0.01)
    asyncio.run(run())


@pytest.mark.parametrize("error", [OSError("uinput failed"), wire.ProtocolError("empty caps")])
def test_partial_device_creation_is_cleaned_up(monkeypatch, error):
    closed = []

    class Device:
        def syn(self):
            pass

        def close(self):
            closed.append(True)

    async def run():
        app = recv.ReceiverApp()

        def create(descriptor):
            if app.uinputs:
                raise error
            app.uinputs.append(Device())
            app.held_per_dev.append(set())

        monkeypatch.setattr(app, "_create_uinput", create)
        reader = asyncio.StreamReader()
        descriptor = wire.DeviceDescriptor(name="kb", vendor_id=0, product_id=0, kind=wire.DEV_KB)
        reader.feed_data(wire.encode_session_open([descriptor, descriptor]))
        assert await app.run(reader) == 1
        assert closed == [True]
        assert not app.uinputs

    asyncio.run(run())
