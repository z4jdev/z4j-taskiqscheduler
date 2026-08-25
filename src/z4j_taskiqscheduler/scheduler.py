"""The :class:`TaskiqSchedulerAdapter` - wraps any taskiq
``ScheduleSource`` (LabelScheduleSource and friends).

LabelScheduleSource walks the broker's task registry and reads
schedule metadata from each task's ``schedule=[]`` decorator
argument. This adapter supports list and read for every compatible source.
Create and update are not advertised because the standard
``LabelScheduleSource`` is decorator-defined and source-controlled. Delete is
advertised and delegated only for custom sources that really implement it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, TypeVar
from uuid import UUID, uuid4

from z4j_core.models import CommandResult, Schedule, ScheduleKind

from z4j_taskiqscheduler.capabilities import DEFAULT_CAPABILITIES

logger = logging.getLogger("z4j.adapter.taskiqscheduler.scheduler")

_NAME = "taskiq-scheduler"
_T = TypeVar("_T")


class TaskiqSchedulerAdapter:
    """Scheduler adapter for taskiq schedule sources.

    Args:
        source: A live ``ScheduleSource`` (e.g.
                ``taskiq.schedule_sources.LabelScheduleSource(broker)``)
                that has been ``startup()``-ed.
        source_loop: Event loop that owns an async custom schedule source.
                AgentRuntime calls adapters on a background loop, so custom
                source operations are marshalled to this verified owner.
                Taskiq's built-in in-memory ``LabelScheduleSource`` is
                loop-neutral and remains usable without this argument.
        project_id: Optional project id used when minting Schedule rows.
    """

    name: str = _NAME

    def __init__(
        self,
        *,
        source: Any,
        source_loop: asyncio.AbstractEventLoop | None = None,
        project_id: UUID | None = None,
    ) -> None:
        self.source = source
        self._source_loop = source_loop
        self._loop_neutral_source = _is_label_schedule_source(source)
        self._project_id = project_id or uuid4()

    async def _await_on_source_loop(
        self,
        operation: Callable[[], Awaitable[_T]],
        *,
        allow_unbound_direct: bool = False,
    ) -> _T:
        """Run one source operation on its fixed owner loop, without retry."""
        owner = self._source_loop
        current = asyncio.get_running_loop()
        if owner is None:
            if allow_unbound_direct:
                return await operation()
            raise RuntimeError(
                "Taskiq custom schedule source event loop is not bound; pass source_loop",
            )
        if owner is current:
            return await operation()
        if owner.is_closed():
            raise RuntimeError("Taskiq schedule source event loop is closed")
        if not owner.is_running():
            raise RuntimeError("Taskiq schedule source event loop is not running")

        async def invoke() -> _T:
            return await operation()

        coroutine = invoke()
        try:
            concurrent_future = asyncio.run_coroutine_threadsafe(coroutine, owner)
        except BaseException:
            coroutine.close()
            raise
        try:
            return await asyncio.wrap_future(concurrent_future)
        except asyncio.CancelledError:
            concurrent_future.cancel()
            raise

    def connect_signals(self, sink: Any) -> None:
        return

    def disconnect_signals(self) -> None:
        return

    async def list_schedules(self) -> list[Schedule]:
        try:
            scheduled = await self._await_on_source_loop(
                self.source.get_schedules,
                allow_unbound_direct=self._loop_neutral_source,
            )
        except Exception:
            # A scheduler snapshot is authoritative: returning [] on a
            # transient source failure tells the brain that every schedule was
            # deleted. Propagate instead so AgentRuntime skips this snapshot
            # and retries on the next reconciliation cycle.
            logger.exception(
                "z4j taskiqscheduler: get_schedules failed; "
                "skipping this snapshot to avoid an authoritative empty inventory"
            )
            raise
        out: list[Schedule] = []
        for sch in scheduled:
            try:
                out.append(self._to_schedule(sch))
            except Exception:
                logger.exception(
                    "z4j taskiqscheduler: failed to map %r; skipping this authoritative snapshot",
                    getattr(sch, "schedule_id", "?"),
                )
                raise
        return out

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        for s in await self.list_schedules():
            if s.external_id == schedule_id or str(s.id) == schedule_id:
                return s
        return None

    async def create_schedule(self, spec: Schedule) -> Schedule:
        raise NotImplementedError(
            "taskiq schedules are decorator-defined; edit your "
            "task's schedule= argument and redeploy.",
        )

    async def update_schedule(
        self,
        schedule_id: str,
        spec: Schedule,
    ) -> Schedule:
        raise NotImplementedError(
            "taskiq schedules are decorator-defined; edit and redeploy.",
        )

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        delete_fn = getattr(self.source, "delete_schedule", None)
        if not callable(delete_fn) or not _source_supports_delete(self.source):
            return CommandResult(
                status="failed",
                error="this taskiq schedule source has no delete_schedule",
            )
        try:
            await self._await_on_source_loop(
                lambda: delete_fn(schedule_id),
            )
        except Exception as exc:
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"schedule_id": schedule_id},
        )

    async def enable_schedule(self, schedule_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error=("taskiq schedules have no enable/disable toggle - delete + re-add to suspend"),
        )

    async def disable_schedule(self, schedule_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error="taskiq schedules have no enable/disable toggle",
        )

    async def trigger_now(self, schedule_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error=(
                "taskiq has no scheduler trigger-now primitive; "
                "kick the underlying task via broker.find_task(...).kiq()"
            ),
        )

    def capabilities(self) -> set[str]:
        capabilities = set(DEFAULT_CAPABILITIES)
        # The standard LabelScheduleSource is read-only, but custom taskiq
        # sources may provide an explicit delete_schedule coroutine. Preserve
        # that working extension point without advertising delete for sources
        # where every request would fail.
        if _source_supports_delete(self.source):
            capabilities.add("delete")
        return capabilities

    def _to_schedule(self, sch: Any) -> Schedule:
        now = datetime.now(UTC)
        sid = uuid4()
        cron_expr = getattr(sch, "cron", None)
        time_at = getattr(sch, "time", None)
        interval = getattr(sch, "interval", None)
        if cron_expr:
            kind = ScheduleKind.CRON
            expression = str(cron_expr)
        elif interval is not None:
            kind = ScheduleKind.INTERVAL
            expression = str(interval)
        elif time_at is not None:
            kind = ScheduleKind.CLOCKED
            expression = time_at.isoformat() if hasattr(time_at, "isoformat") else str(time_at)
        else:
            kind = ScheduleKind.CRON
            expression = "unknown"
        return Schedule(
            id=sid,
            project_id=self._project_id,
            engine="taskiq",
            scheduler=self.name,
            name=getattr(sch, "task_name", None) or "taskiq-schedule",
            task_name=getattr(sch, "task_name", None) or "taskiq-task",
            kind=kind,
            expression=expression,
            timezone="UTC",
            args=list(getattr(sch, "args", []) or []),
            kwargs=dict(getattr(sch, "kwargs", {}) or {}),
            is_enabled=True,
            external_id=getattr(sch, "schedule_id", None) or str(sid),
            created_at=now,
            updated_at=now,
        )


def _source_supports_delete(source: Any) -> bool:
    """Return true only when a source overrides taskiq's failing default."""
    delete_fn = getattr(source, "delete_schedule", None)
    if not callable(delete_fn):
        return False
    try:
        from taskiq.abc.schedule_source import ScheduleSource
    except ImportError:  # pragma: no cover - taskiq is a required dependency
        return True
    return getattr(type(source), "delete_schedule", None) is not ScheduleSource.delete_schedule


def _is_label_schedule_source(source: Any) -> bool:
    """Return whether ``source`` is Taskiq's loop-neutral label source."""
    try:
        from taskiq.schedule_sources import LabelScheduleSource
    except ImportError:  # pragma: no cover - taskiq is a required dependency
        return False
    # A subclass may override get_schedules with loop-bound database or network
    # I/O, so only the exact stock in-memory implementation gets this fallback.
    return type(source) is LabelScheduleSource


__all__ = ["TaskiqSchedulerAdapter"]
