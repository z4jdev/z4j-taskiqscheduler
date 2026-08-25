# z4j-taskiqscheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiqscheduler.svg)](https://pypi.org/project/z4j-taskiqscheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiqscheduler.svg)](https://pypi.org/project/z4j-taskiqscheduler/)
[![License](https://img.shields.io/pypi/l/z4j-taskiqscheduler.svg)](https://github.com/z4jdev/z4j-taskiqscheduler/blob/main/LICENSE)

The taskiq-scheduler adapter for [z4j](https://z4j.com).

Surfaces taskiq-scheduler periodic jobs on the dashboard's Schedules
page as list/read inventory. Delete is advertised only for a custom source
that overrides taskiq's unsupported `delete_schedule` default.

## Compatibility

- TaskIQ 0.11+ and <1
- Python 3.11+

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| List schedules | jobs returned by the configured taskiq-scheduler source |
| Read | by registered name |
| Delete | advertised only when the configured source implements it; label-defined schedules require a code change and redeploy |
| Boot inventory | full snapshot at agent connect; existing schedules show up without editing |

taskiq-scheduler schedules are typically defined declaratively (label
source, JSON file, or label decorators), so create / update are
intentionally out of scope, those need a deploy round-trip. taskiq
also has no enable/disable toggle or trigger-now primitive, so the
dashboard hides those actions for this adapter.

## Install

```bash
pip install z4j-taskiq z4j-taskiqscheduler
```

```python
import asyncio
import logging
import os

from taskiq import TaskiqEvents, TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource
from z4j_bare import install_agent
from z4j_taskiq import TaskiqEngineAdapter, attach_to_broker
from z4j_taskiqscheduler import TaskiqSchedulerAdapter

source = LabelScheduleSource(broker)
scheduler = TaskiqScheduler(broker=broker, sources=[source])
engine = TaskiqEngineAdapter(broker=broker)
attach_to_broker(broker, adapter=engine)
_z4j_installed = False
logger = logging.getLogger(__name__)


@broker.on_event(TaskiqEvents.CLIENT_STARTUP)
async def install_z4j_after_scheduler_sources(_state) -> None:
    global _z4j_installed
    if _z4j_installed:
        return
    owner_loop = asyncio.get_running_loop()
    try:
        scheduler_adapter = TaskiqSchedulerAdapter(
            source=source,
            source_loop=owner_loop,
        )
        install_agent(
            engines=[engine],
            schedulers=[scheduler_adapter],
            brain_url="https://brain.example.com",
            token="z4j_agent_...",
            project_id="my-project",
            hmac_secret=os.environ["Z4J_HMAC_SECRET"],
        )
    except Exception as exc:
        logger.error(
            "z4j TaskIQ scheduler install failed (%s)",
            type(exc).__name__,
        )
        return
    _z4j_installed = True
```

Run this module with TaskIQ's scheduler CLI. It starts each configured source
before calling broker startup, so `CLIENT_STARTUP` installs z4j on the broker's
real owner loop after this `LabelScheduleSource` is live. The guard makes a
repeated callback reuse the same adapter and agent. The CLI owns broker startup
and shutdown; do not call either lifecycle method from this module.

For a custom async schedule source, always pass that same verified loop as
`source_loop`. z4j marshals `get_schedules` and supported `delete_schedule`
calls to it once, without retrying an ambiguous delete. Only the exact stock
`LabelScheduleSource`, whose read is an in-memory registry snapshot, supports
an unbound direct read; custom sources fail closed until an owner is supplied.

## Pairs with

- [`z4j-taskiq`](https://github.com/z4jdev/z4j-taskiq), engine adapter

## Reliability

- Inventory failures skip the snapshot rather than publishing an authoritative
  empty schedule set.
- Custom source reads and deletes stay on their explicit owner event loop;
  cancellation is forwarded and z4j does not retry the operation.
- The adapter does not rewrite schedule definitions. Delete is advertised and
  delegated only when a custom source overrides taskiq's failing default; the
  standard label source remains read-only.

## Documentation

Full docs at [z4j.dev/schedulers/taskiq-scheduler/](https://z4j.dev/schedulers/taskiq-scheduler/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-taskiqscheduler/
- Issues: https://github.com/z4jdev/z4j-taskiqscheduler/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
