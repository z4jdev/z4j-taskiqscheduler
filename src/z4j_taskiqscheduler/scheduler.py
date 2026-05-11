"""The :class:`TaskiqSchedulerAdapter` - wraps any taskiq
``ScheduleSource`` (LabelScheduleSource and friends).

LabelScheduleSource walks the broker's task registry and reads
schedule metadata from each task's ``schedule=[]`` decorator
argument. It supports list / read / delete; create + update are
not part of its contract (schedules are decorator-defined,
source-controlled).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from z4j_core.models import CommandResult, Schedule, ScheduleKind

from z4j_taskiqscheduler.capabilities import DEFAULT_CAPABILITIES

logger = logging.getLogger("z4j.adapter.taskiqscheduler.scheduler")

_NAME = "taskiq-scheduler"


class TaskiqSchedulerAdapter:
    """Scheduler adapter for taskiq schedule sources.

    Args:
        source: A live ``ScheduleSource`` (e.g.
                ``taskiq.schedule_sources.LabelScheduleSource(broker)``)
                that has been ``startup()``-ed.
        project_id: Optional project id used when minting Schedule rows.
    """

    name: str = _NAME

    def __init__(
        self,
        *,
        source: Any,
        project_id: UUID | None = None,
    ) -> None:
        self.source = source
        self._project_id = project_id or uuid4()

    def connect_signals(self, sink: Any) -> None:  # noqa: ARG002
        return

    def disconnect_signals(self) -> None:
        return

    async def list_schedules(self) -> list[Schedule]:
        try:
            scheduled = await self.source.get_schedules()
        except Exception:  # noqa: BLE001
            logger.exception("z4j taskiqscheduler: get_schedules failed")
            return []
        out: list[Schedule] = []
        for sch in scheduled:
            try:
                out.append(self._to_schedule(sch))
            except Exception:  # noqa: BLE001
                logger.exception(
                    "z4j taskiqscheduler: failed to map %r",
                    getattr(sch, "schedule_id", "?"),
                )
        return out

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        for s in await self.list_schedules():
            if s.external_id == schedule_id or str(s.id) == schedule_id:
                return s
        return None

    async def create_schedule(self, spec: Schedule) -> Schedule:  # noqa: ARG002
        raise NotImplementedError(
            "taskiq schedules are decorator-defined; edit your "
            "task's schedule= argument and redeploy.",
        )

    async def update_schedule(
        self, schedule_id: str, spec: Schedule,  # noqa: ARG002
    ) -> Schedule:
        raise NotImplementedError(
            "taskiq schedules are decorator-defined; edit and redeploy.",
        )

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        delete_fn = getattr(self.source, "delete_schedule", None)
        if delete_fn is None:
            return CommandResult(
                status="failed",
                error="this taskiq schedule source has no delete_schedule",
            )
        try:
            await delete_fn(schedule_id)
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"schedule_id": schedule_id},
        )

    async def enable_schedule(self, schedule_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(
            status="failed",
            error=(
                "taskiq schedules have no enable/disable toggle - "
                "delete + re-add to suspend"
            ),
        )

    async def disable_schedule(self, schedule_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(
            status="failed",
            error="taskiq schedules have no enable/disable toggle",
        )

    async def trigger_now(self, schedule_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(
            status="failed",
            error=(
                "taskiq has no scheduler trigger-now primitive; "
                "kick the underlying task via broker.find_task(...).kiq()"
            ),
        )

    def capabilities(self) -> set[str]:
        return set(DEFAULT_CAPABILITIES)

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
            expression = (
                time_at.isoformat()
                if hasattr(time_at, "isoformat")
                else str(time_at)
            )
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


__all__ = ["TaskiqSchedulerAdapter"]
