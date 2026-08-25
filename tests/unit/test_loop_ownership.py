"""Event-loop ownership contracts for custom Taskiq schedule sources."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable, Generator
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker
from taskiq.schedule_sources import LabelScheduleSource
from z4j_taskiqscheduler import TaskiqSchedulerAdapter


class _OwnerLoopThread:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread_id: int | None = None
        self._started = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self._started.wait(timeout=2), "source owner loop did not start"

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.thread_id = threading.get_ident()
        self._started.set()
        self.loop.run_forever()
        self.loop.close()

    def close(self) -> None:
        if self.loop.is_closed():
            return
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=2)
        assert not self._thread.is_alive(), "source owner loop did not stop"


@pytest.fixture
def owner_loop() -> Generator[_OwnerLoopThread, None, None]:
    owner = _OwnerLoopThread()
    try:
        yield owner
    finally:
        owner.close()


def _schedule(schedule_id: str = "schedule-1") -> SimpleNamespace:
    return SimpleNamespace(
        schedule_id=schedule_id,
        task_name="jobs.cleanup",
        cron="*/5 * * * *",
        interval=None,
        time=None,
        args=[],
        kwargs={},
    )


class _LoopBoundSource:
    def __init__(self, owner: Any) -> None:
        self._owner = owner
        self.get_threads: list[int] = []
        self.delete_threads: list[int] = []

    def _record(self, calls: list[int]) -> None:
        if asyncio.get_running_loop() is not self._owner.loop:
            raise RuntimeError("schedule source called from a different loop")
        calls.append(threading.get_ident())

    async def get_schedules(self) -> list[SimpleNamespace]:
        self._record(self.get_threads)
        return [_schedule()]

    async def delete_schedule(self, _schedule_id: str) -> None:
        self._record(self.delete_threads)


@pytest.mark.asyncio
async def test_list_and_get_marshal_to_custom_source_owner_loop(
    owner_loop: _OwnerLoopThread,
) -> None:
    source = _LoopBoundSource(owner_loop)
    adapter = TaskiqSchedulerAdapter(
        source=source,
        source_loop=owner_loop.loop,
    )

    rows = await adapter.list_schedules()
    found = await adapter.get_schedule("schedule-1")

    assert len(rows) == 1
    assert found is not None
    assert found.external_id == "schedule-1"
    assert source.get_threads == [owner_loop.thread_id, owner_loop.thread_id]


@pytest.mark.asyncio
async def test_delete_marshals_to_custom_source_owner_loop(
    owner_loop: _OwnerLoopThread,
) -> None:
    source = _LoopBoundSource(owner_loop)
    adapter = TaskiqSchedulerAdapter(
        source=source,
        source_loop=owner_loop.loop,
    )

    result = await adapter.delete_schedule("schedule-1")

    assert result.status == "success"
    assert source.delete_threads == [owner_loop.thread_id]


@pytest.mark.asyncio
async def test_same_loop_source_operations_do_not_use_thread_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = SimpleNamespace(loop=asyncio.get_running_loop())
    source = _LoopBoundSource(owner)
    adapter = TaskiqSchedulerAdapter(source=source, source_loop=owner.loop)

    def unexpected_bridge(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("same-loop source operation used thread bridge")

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", unexpected_bridge)

    assert len(await adapter.list_schedules()) == 1
    assert (await adapter.delete_schedule("schedule-1")).status == "success"
    assert source.get_threads == [threading.get_ident()]
    assert source.delete_threads == [threading.get_ident()]


@pytest.mark.asyncio
async def test_unbound_custom_source_fails_before_any_source_call() -> None:
    owner = SimpleNamespace(loop=asyncio.get_running_loop())
    source = _LoopBoundSource(owner)
    adapter = TaskiqSchedulerAdapter(source=source)

    with pytest.raises(RuntimeError, match="event loop is not bound"):
        await adapter.list_schedules()
    with pytest.raises(RuntimeError, match="event loop is not bound"):
        await adapter.get_schedule("schedule-1")
    deleted = await adapter.delete_schedule("schedule-1")

    assert deleted.status == "failed"
    assert "event loop is not bound" in (deleted.error or "")
    assert source.get_threads == []
    assert source.delete_threads == []


@pytest.mark.asyncio
async def test_label_source_subclass_does_not_get_unbound_direct_fallback() -> None:
    class CustomLabelSource(LabelScheduleSource):
        pass

    source = CustomLabelSource(InMemoryBroker())
    adapter = TaskiqSchedulerAdapter(source=source)

    with pytest.raises(RuntimeError, match="event loop is not bound"):
        await adapter.list_schedules()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["closed", "stopped"])
async def test_unavailable_owner_fails_before_any_source_call(state: str) -> None:
    owner_loop = asyncio.new_event_loop()
    if state == "closed":
        owner_loop.close()
    source = _LoopBoundSource(SimpleNamespace(loop=owner_loop))
    adapter = TaskiqSchedulerAdapter(source=source, source_loop=owner_loop)
    error_state = "closed" if state == "closed" else "not running"

    try:
        with pytest.raises(RuntimeError, match=f"event loop is {error_state}"):
            await adapter.list_schedules()
        deleted = await adapter.delete_schedule("schedule-1")
        assert deleted.status == "failed"
        assert f"event loop is {error_state}" in (deleted.error or "")
        assert source.get_threads == []
        assert source.delete_threads == []
    finally:
        if not owner_loop.is_closed():
            owner_loop.close()


class _BlockingSource(_LoopBoundSource):
    def __init__(self, owner: Any) -> None:
        super().__init__(owner)
        self.get_started = threading.Event()
        self.get_cancelled = threading.Event()
        self.delete_started = threading.Event()
        self.delete_cancelled = threading.Event()

    async def get_schedules(self) -> list[SimpleNamespace]:
        self._record(self.get_threads)
        self.get_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.get_cancelled.set()
            raise
        return []

    async def delete_schedule(self, _schedule_id: str) -> None:
        self._record(self.delete_threads)
        self.delete_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.delete_cancelled.set()
            raise


@pytest.mark.asyncio
async def test_list_cancellation_reaches_owner_without_retry(
    owner_loop: _OwnerLoopThread,
) -> None:
    source = _BlockingSource(owner_loop)
    adapter = TaskiqSchedulerAdapter(source=source, source_loop=owner_loop.loop)
    pending = asyncio.create_task(adapter.list_schedules())

    assert await asyncio.to_thread(source.get_started.wait, 2)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending

    assert await asyncio.to_thread(source.get_cancelled.wait, 2)
    assert source.get_threads == [owner_loop.thread_id]


@pytest.mark.asyncio
async def test_delete_cancellation_reaches_owner_without_retry(
    owner_loop: _OwnerLoopThread,
) -> None:
    source = _BlockingSource(owner_loop)
    adapter = TaskiqSchedulerAdapter(source=source, source_loop=owner_loop.loop)
    pending = asyncio.create_task(adapter.delete_schedule("schedule-1"))

    assert await asyncio.to_thread(source.delete_started.wait, 2)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending

    assert await asyncio.to_thread(source.delete_cancelled.wait, 2)
    assert source.delete_threads == [owner_loop.thread_id]


@pytest.mark.asyncio
async def test_no_hop_negative_reproduces_cross_loop_source_failure(
    owner_loop: _OwnerLoopThread,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _LoopBoundSource(owner_loop)
    adapter = TaskiqSchedulerAdapter(source=source, source_loop=owner_loop.loop)

    async def direct(
        operation: Callable[[], Awaitable[Any]],
        **_kwargs: object,
    ) -> Any:
        return await operation()

    monkeypatch.setattr(adapter, "_await_on_source_loop", direct)

    with pytest.raises(RuntimeError, match="different loop"):
        await adapter.list_schedules()
    deleted = await adapter.delete_schedule("schedule-1")
    assert deleted.status == "failed"
    assert "different loop" in (deleted.error or "")
