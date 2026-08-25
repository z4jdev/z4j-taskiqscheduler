"""TaskiqSchedulerAdapter tests using LabelScheduleSource."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker
from taskiq.schedule_sources import LabelScheduleSource
from z4j_taskiqscheduler import TaskiqSchedulerAdapter


@pytest.fixture
def broker():
    b = InMemoryBroker()

    @b.task(schedule=[{"cron": "*/5 * * * *"}])
    async def cleanup():
        return "ok"

    @b.task(schedule=[{"cron": "0 3 * * *"}])
    async def nightly():
        return "ok"

    return b


@pytest.fixture
async def source(broker):
    s = LabelScheduleSource(broker)
    await s.startup()
    yield s
    await s.shutdown()


@pytest.fixture
def adapter(source):
    return TaskiqSchedulerAdapter(source=source)


@pytest.mark.asyncio
async def test_lists_decorator_defined_schedules(adapter):
    rows = await adapter.list_schedules()
    assert len(rows) >= 2
    crons = {r.expression for r in rows}
    assert "*/5 * * * *" in crons
    assert "0 3 * * *" in crons


@pytest.mark.asyncio
async def test_source_failure_propagates_instead_of_emitting_empty_snapshot():
    class FailingSource:
        async def get_schedules(self):
            raise ConnectionError("temporary taskiq source outage")

    adapter = TaskiqSchedulerAdapter(
        source=FailingSource(),
        source_loop=asyncio.get_running_loop(),
    )

    # AgentRuntime treats a returned list as authoritative and catches a
    # raised exception by skipping the snapshot. The old return-[] behavior
    # therefore represented every existing schedule as deleted.
    with pytest.raises(ConnectionError, match="temporary taskiq source outage"):
        await adapter.list_schedules()


@pytest.mark.asyncio
async def test_mapping_failure_aborts_authoritative_snapshot(adapter, monkeypatch):
    original = adapter._to_schedule
    calls = 0

    def fail_second(schedule):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError("malformed schedule")
        return original(schedule)

    monkeypatch.setattr(adapter, "_to_schedule", fail_second)
    with pytest.raises(ValueError, match="malformed schedule"):
        await adapter.list_schedules()


@pytest.mark.asyncio
async def test_engine_and_scheduler_set(adapter):
    rows = await adapter.list_schedules()
    assert all(r.engine == "taskiq" for r in rows)
    assert all(r.scheduler == "taskiq-scheduler" for r in rows)


def test_standard_source_does_not_advertise_delete(adapter):
    assert adapter.capabilities() == {"list", "read"}


def test_custom_source_advertises_delete_when_it_implements_delete_schedule():
    class CustomSource:
        async def get_schedules(self):
            return []

        async def delete_schedule(self, schedule_id):
            return None

    adapter = TaskiqSchedulerAdapter(source=CustomSource())
    assert adapter.capabilities() == {"list", "read", "delete"}


@pytest.mark.asyncio
async def test_standard_source_delete_fails_closed(adapter):
    row = (await adapter.list_schedules())[0]
    result = await adapter.delete_schedule(row.external_id)
    assert result.status == "failed"


@pytest.mark.asyncio
async def test_custom_source_delete_delegates_to_supported_extension():
    deleted: list[str] = []

    class CustomSource:
        async def get_schedules(self):
            return []

        async def delete_schedule(self, schedule_id):
            deleted.append(schedule_id)

    adapter = TaskiqSchedulerAdapter(
        source=CustomSource(),
        source_loop=asyncio.get_running_loop(),
    )
    result = await adapter.delete_schedule("custom-1")

    assert result.status == "success"
    assert result.result == {"schedule_id": "custom-1"}
    assert deleted == ["custom-1"]


@pytest.mark.asyncio
async def test_get_by_external_id(adapter):
    rows = await adapter.list_schedules()
    target = rows[0]
    found = await adapter.get_schedule(target.external_id)
    assert found is not None


@pytest.mark.asyncio
async def test_create_raises_not_implemented(adapter):
    with pytest.raises(NotImplementedError):
        await adapter.create_schedule(spec=None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_enable_clearly_unsupported(adapter):
    res = await adapter.enable_schedule("anything")
    assert res.status == "failed"


@pytest.mark.asyncio
async def test_trigger_clearly_unsupported(adapter):
    res = await adapter.trigger_now("anything")
    assert res.status == "failed"
