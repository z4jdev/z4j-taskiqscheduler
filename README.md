# z4j-taskiqscheduler

[![PyPI version](https://img.shields.io/pypi/v/z4j-taskiqscheduler.svg)](https://pypi.org/project/z4j-taskiqscheduler/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-taskiqscheduler.svg)](https://pypi.org/project/z4j-taskiqscheduler/)
[![License](https://img.shields.io/pypi/l/z4j-taskiqscheduler.svg)](https://github.com/z4jdev/z4j-taskiqscheduler/blob/main/LICENSE)

The taskiq-scheduler adapter for [z4j](https://z4j.com).

Surfaces taskiq-scheduler periodic jobs on the dashboard's Schedules
page, read, enable, disable, trigger.

## What it ships

| Capability | Notes |
|---|---|
| List schedules | every job registered with the taskiq-scheduler source |
| Read | by registered name |
| Enable / disable | via consumer-side gating |
| Trigger now | enqueues the task immediately, outside the schedule |
| Boot inventory | full snapshot at agent connect; existing schedules show up without editing |

taskiq-scheduler schedules are typically defined declaratively (label
source, JSON file, or label decorators), so create / update / delete
are intentionally out of scope, those need a deploy round-trip.

## Install

```bash
pip install z4j-taskiq z4j-taskiqscheduler
```

```python
from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource
from z4j_bare import install_agent
from z4j_taskiq import TaskiqEngineAdapter
from z4j_taskiqscheduler import TaskiqSchedulerAdapter

scheduler = TaskiqScheduler(
    broker=broker,
    sources=[LabelScheduleSource(broker)],
)

install_agent(
    engines=[TaskiqEngineAdapter(broker=broker)],
    schedulers=[TaskiqSchedulerAdapter(scheduler=scheduler)],
    brain_url="https://brain.example.com",
    token="z4j_agent_...",
    project_id="my-project",
)
```

## Pairs with

- [`z4j-taskiq`](https://github.com/z4jdev/z4j-taskiq), engine adapter

## Reliability

- No exception from the adapter ever propagates back into
  taskiq-scheduler or your task code.
- Schedule sources are read-only at runtime; the adapter only
  observes, it does not rewrite the underlying source.

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
