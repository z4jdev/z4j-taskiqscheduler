"""TaskiqSchedulerAdapter tests using LabelScheduleSource."""

from __future__ import annotations

import pytest

pytest.importorskip("taskiq")

from taskiq import InMemoryBroker  # noqa: E402
from taskiq.schedule_sources import LabelScheduleSource  # noqa: E402

from z4j_taskiqscheduler import TaskiqSchedulerAdapter  # noqa: E402


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
async def test_engine_and_scheduler_set(adapter):
    rows = await adapter.list_schedules()
    assert all(r.engine == "taskiq" for r in rows)
    assert all(r.scheduler == "taskiq-scheduler" for r in rows)


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
